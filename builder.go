package sqlol

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lovego/bsql"
)

type Builder struct {
	manipulation     string
	table            string
	tableAlias       string
	join             []string
	groupBy          []string
	orderBy          []string
	having           string
	limit            int64
	offset           int64
	isForUpdate      bool
	fields           []string
	cols             []string
	returning        []string
	onConflict       string
	values           []interface{}
	updates          []string
	updateStruct     interface{}
	ConditionBuilder ConditionBuilder
}

func NewBuilder() *Builder {
	return &Builder{ConditionBuilder: ConditionBuilder{}}
}

func (b *Builder) Clone() *Builder {
	values := make([]interface{}, len(b.values))
	copy(values, b.values)
	return &Builder{
		manipulation: b.manipulation,
		table:        b.table,
		tableAlias:   b.tableAlias,
		join:         copyStringSlice(b.join),
		groupBy:      copyStringSlice(b.groupBy),
		orderBy:      copyStringSlice(b.orderBy),
		having:       b.having,
		limit:        b.limit,
		offset:       b.offset,
		isForUpdate:  b.isForUpdate,
		fields:       copyStringSlice(b.fields),
		cols:         copyStringSlice(b.cols),
		returning:    copyStringSlice(b.returning),
		onConflict:   b.onConflict,
		values:       values,
		updates:      copyStringSlice(b.updates),
		updateStruct: b.updateStruct,
		ConditionBuilder: ConditionBuilder{
			wheres: copyStringSlice(b.ConditionBuilder.wheres),
		},
	}
}

func (b *Builder) Clear() {
	b.manipulation = ""
	b.table = ""
	b.tableAlias = ""
	b.join = nil
	b.groupBy = nil
	b.orderBy = nil
	b.having = ""
	b.limit = 0
	b.offset = 0
	b.isForUpdate = false
	b.fields = nil
	b.cols = nil
	b.returning = nil
	b.onConflict = ""
	b.values = nil
	b.updates = nil
	b.updateStruct = nil
	b.ConditionBuilder.Clear()
}

func (b *Builder) Insert(table string) *Builder {
	b.manipulation = manipulationInsert
	b.table = table
	return b
}

func (b *Builder) Update(table string) *Builder {
	b.manipulation = manipulationUpdate
	b.table = table
	return b
}
func (b *Builder) Delete(table string) *Builder {
	b.manipulation = manipulationDelete
	b.table = table
	return b
}

func (b *Builder) Select(table string) *Builder {
	b.manipulation = manipulationSelect
	b.table = table
	return b
}

func (b *Builder) SelectSubQuery(subQuery string) *Builder {
	b.manipulation = manipulationSelect
	b.table = "(" + subQuery + ")"
	return b
}

func (b *Builder) Alias(alias string) *Builder {
	b.tableAlias = alias
	return b
}

func (b *Builder) OrderBy(order ...string) *Builder {
	b.orderBy = append(b.orderBy, order...)
	return b
}

func (b *Builder) Limit(limit int64) *Builder {
	b.limit = limit
	return b
}

func (b *Builder) Offset(offset int64) *Builder {
	b.offset = offset
	return b
}

// 添加自定义sql策略 Strategy接口形式
func (b *Builder) Strategies(strategies ...Strategy) *Builder {
	for _, strategy := range strategies {
		if strategy != nil {
			strategy.Execute(b)
		}
	}
	return b
}

type StrategyFunc func(b *Builder)

// 添加自定义sql策略 回调函数形式
func (b *Builder) StrategyFuncs(strategyFuncs ...StrategyFunc) *Builder {
	for _, strategyFunc := range strategyFuncs {
		if strategyFunc != nil {
			strategyFunc(b)
		}
	}
	return b
}

func (b *Builder) Build() string {
	if b.table == "" {
		log.Panic("sql builder: table is required")
		return ""
	}
	switch b.manipulation {
	case manipulationSelect:
		return b.query()
	case manipulationInsert:
		return b.insert()
	case manipulationUpdate:
		return b.update()
	case manipulationDelete:
		return b.delete()
	default:
		log.Panic("sql builder: wrong manipulation")
		return ""
	}
}

func (b *Builder) BuildCount() string {
	if b.table == "" {
		log.Panic("sql builder: table is required")
		return ""
	}
	if b.manipulation != manipulationSelect {
		log.Panic("sql builder: must be a select operation")
		return ""
	}
	if len(b.groupBy) == 0 {
		return strings.Join([]string{
			b.manipulation,
			"COUNT(1) FROM",
			b.tableName(),
			b.buildJoin(),
			b.buildWhere(),
		}, " ")
	}
	if len(b.groupBy) == 1 &&
		b.having == "" &&
		!strings.Contains(b.groupBy[0], ",") {
		return strings.Join([]string{
			b.manipulation,
			fmt.Sprintf("COUNT(DISTINCT %s) FROM", b.groupBy[0]),
			b.tableName(),
			b.buildJoin(),
			b.buildWhere(),
		}, " ")
	}
	subSql := strings.Join([]string{
		b.selectFields(),
		"FROM",
		b.tableName(),
		b.buildJoin(),
		b.buildWhere(),
		b.buildGroup(),
		b.buildHaving(),
	}, " ")
	return fmt.Sprintf(`SELECT count(1) FROM (%s) AS T`, subSql)
}

func (b *Builder) buildWhere() string {
	condition := b.ConditionBuilder.Build()
	if condition != "" {
		condition = "WHERE " + condition
	}
	return condition
}

func (b *Builder) tableName() string {
	table := b.table
	if b.tableAlias != "" {
		table += " AS " + b.tableAlias
	}
	return table
}

func (b *Builder) buildOrder() string {
	if len(b.orderBy) == 0 {
		return ""
	}
	return "ORDER BY " + strings.Join(b.orderBy, ",")
}

func (b *Builder) buildLimit() string {
	if b.limit <= 0 {
		return ""
	}
	sql := fmt.Sprintf("LIMIT %d", b.limit)
	if b.offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", b.offset)
	}
	return sql
}

func (b *Builder) query() string {
	return strings.Join([]string{
		b.selectFields(),
		"FROM",
		b.tableName(),
		b.buildJoin(),
		b.buildWhere(),
		b.buildGroup(),
		b.buildHaving(),
		b.buildOrder(),
		b.buildLimit(),
		b.buildForUpdate(),
	}, " ")
}

func (b *Builder) Join(joinType, table, as, on string) *Builder {
	b.join = append(b.join,
		fmt.Sprintf("%s JOIN %s AS %s ON %s", joinType, table, as, on))
	return b
}

func (b *Builder) LeftJoin(table, as, on string) *Builder {
	return b.Join("LEFT", table, as, on)
}

func (b *Builder) RightJoin(table, as, on string) *Builder {
	return b.Join("RIGHT", table, as, on)
}

func (b *Builder) InnerJoin(table, as, on string) *Builder {
	return b.Join("INNER", table, as, on)
}

func (b *Builder) GroupBy(group ...string) *Builder {
	b.groupBy = append(b.groupBy, group...)
	return b
}

func (b *Builder) Having(having string) *Builder {
	b.having = having
	return b
}

func (b *Builder) Fields(fields ...string) *Builder {
	b.fields = append(b.fields, fields...)
	return b
}
func (b *Builder) ForUpdate() *Builder {
	b.isForUpdate = true
	return b
}

func (b *Builder) buildForUpdate() string {
	if b.isForUpdate {
		return "FOR UPDATE"
	}
	return ""
}

func (b *Builder) buildJoin() string {
	if len(b.join) == 0 {
		return ""
	}
	return strings.Join(b.join, " ")
}
func (b *Builder) buildGroup() string {
	if len(b.groupBy) == 0 {
		return ""
	}
	return "GROUP BY " + strings.Join(b.groupBy, ",")
}

func (b *Builder) buildHaving() string {
	if b.having == "" {
		return ""
	}
	return "HAVING " + b.having
}

func (b *Builder) selectFields() string {
	fields := "*"
	if len(b.fields) > 0 {
		fields = strings.Join(b.fields, ",")
	}
	return fmt.Sprintf("%s %s", b.manipulation, fields)
}

func (b *Builder) Cols(cols ...string) *Builder {
	b.cols = append(b.cols, cols...)
	return b
}

func (b *Builder) Set(data ...string) *Builder {
	b.updates = append(b.updates, data...)
	return b
}

func (b *Builder) SetMap(data map[string]interface{}) *Builder {
	for k, v := range data {
		b.updates = append(b.updates,
			fmt.Sprintf("%s = %s", k, bsql.V(v)))
	}
	return b
}

func (b *Builder) SetStruct(data interface{}) *Builder {
	b.updateStruct = data
	return b
}

func (b *Builder) Values(values []interface{}) *Builder {
	b.values = values
	return b
}

func (b *Builder) insert() string {
	if len(b.values) == 0 {
		log.Panic("sql builder: inserting values are required")
		return ""
	}
	cols := b.insertCols()
	if len(cols) == 0 {
		log.Panic("sql builder: inserting fields are required")
		return ""
	}
	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s %s %s",
		b.tableName(),
		strings.Join(bsql.Fields2Columns(cols), ","),
		bsql.StructValues(b.values, cols),
		b.onConflict,
		b.buildReturning(),
	)
}

func (b *Builder) update() string {
	return strings.Join([]string{
		b.manipulation,
		b.tableName(),
		"SET",
		b.buildUpdates(),
		b.buildWhere(),
		b.buildOrder(),
		b.buildLimit(),
		b.onConflict,
		b.buildReturning(),
	}, " ")
}

func (b *Builder) delete() string {
	where := b.buildWhere()
	if where == "" {
		log.Panic("sql builder: deleting condition are required")
		return ""
	}
	return strings.Join([]string{
		b.manipulation,
		"FROM",
		b.tableName(),
		where,
		b.buildOrder(),
		b.buildLimit(),
		b.onConflict,
		b.buildReturning(),
	}, " ")
}

func (b *Builder) buildUpdates() string {
	if b.updateStruct != nil {
		cols := b.updateCols()
		return fmt.Sprintf("(%s) = %s",
			strings.Join(bsql.Fields2Columns(cols), ","),
			bsql.StructValues([]interface{}{b.updateStruct}, cols))
	}
	if len(b.updates) == 0 {
		log.Panic("sql builder: updating values are required")
		return ""
	}
	return strings.Join(b.updates, ",")
}

func (b *Builder) buildReturning() string {
	if len(b.returning) == 0 {
		return ""
	}
	return strings.Join(b.returning, ",")
}

func (b *Builder) insertCols() []string {
	cols := b.cols
	if len(cols) == 0 {
		s := b.values[0]
		cols = bsql.FieldsFromStruct(s,
			[]string{"Id", "UpdatedBy", "UpdatedAt"})
	}
	return cols
}

func (b *Builder) updateCols() []string {
	cols := b.cols
	if len(cols) == 0 {
		cols = bsql.FieldsFromStruct(b.updateStruct,
			[]string{"CreatedBy", "CreatedAt"})
	}
	return cols
}

func (b *Builder) OnConflict(fields string, do string) *Builder {
	if fields == "" {
		b.onConflict = "ON CONFLICT DO " + do
	} else {
		b.onConflict = fmt.Sprintf("ON CONFLICT (%s) DO %s", fields, do)
	}
	return b
}

func (b *Builder) OnConflictDoNothing() *Builder {
	return b.OnConflict("", "NOTHING")
}

func (b *Builder) Returning(fields ...string) *Builder {
	b.returning = append(b.returning, fields...)
	return b
}

func (b *Builder) Where(strs ...string) *Builder {
	b.ConditionBuilder.Where(strs...)
	return b
}

func (b *Builder) WhereMap(where map[string]interface{}) *Builder {
	b.ConditionBuilder.WhereMap(where)
	return b
}

func (b *Builder) TryMap(where map[string]interface{}) *Builder {
	b.ConditionBuilder.TryMap(where)
	return b
}

func (b *Builder) Or(strs ...string) *Builder {
	b.ConditionBuilder.Or(strs...)
	return b
}

func (b *Builder) Equal(dbField string, value interface{}) *Builder {
	b.ConditionBuilder.Equal(dbField, value)
	return b
}

func (b *Builder) TryEqual(dbField string, value interface{}) *Builder {
	b.ConditionBuilder.TryEqual(dbField, value)
	return b
}

func (b *Builder) Like(dbField, value string) *Builder {
	b.ConditionBuilder.Like(dbField, value)
	return b
}

func (b *Builder) TryLike(dbField string, value string) *Builder {
	b.ConditionBuilder.TryLike(dbField, value)
	return b
}

func (b *Builder) MultiLike(dbFields []string, value string) *Builder {
	b.ConditionBuilder.MultiLike(dbFields, value)
	return b
}

func (b *Builder) TryMultiLike(dbFields []string, value string) *Builder {
	b.ConditionBuilder.TryMultiLike(dbFields, value)
	return b
}

func (b *Builder) Between(
	dbField string, start, end interface{}) *Builder {
	b.ConditionBuilder.Between(dbField, start, end)
	return b
}

func (b *Builder) In(dbField string, values interface{}) *Builder {
	b.ConditionBuilder.In(dbField, values)
	return b
}

func (b *Builder) TryIn(dbField string, values interface{}) *Builder {
	b.ConditionBuilder.TryIn(dbField, values)
	return b
}

func (b *Builder) NotIn(dbField string, values interface{}) *Builder {
	b.ConditionBuilder.NotIn(dbField, values)
	return b
}

func (b *Builder) Any(dbField string, values interface{}) *Builder {
	b.ConditionBuilder.Any(dbField, values)
	return b
}

func (b *Builder) TryAny(dbField string, values interface{}) *Builder {
	b.ConditionBuilder.TryAny(dbField, values)
	return b
}

func (b *Builder) TryTimeRange(dbField string, startTime, endTime time.Time) *Builder {
	b.ConditionBuilder.TryTimeRange(dbField, startTime, endTime)
	return b
}

func (b *Builder) TryDateRange(dbField string, startDate, endDate time.Time) *Builder {
	b.ConditionBuilder.TryDateRange(dbField, startDate, endDate)
	return b
}

func copyStringSlice(src []string) []string {
	res := make([]string, len(src))
	copy(res, src)
	return res
}

const (
	manipulationInsert = "INSERT"
	manipulationDelete = "DELETE"
	manipulationUpdate = "UPDATE"
	manipulationSelect = "SELECT"

	TimeLayout = "2006-01-02 15:04:05"
	DateLayout = "2006-01-02"
)
