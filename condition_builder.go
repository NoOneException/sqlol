package sqlol

import (
	"fmt"
	"strings"
	"time"
)

type ConditionBuilder struct {
	wheres []string
}

// 生成最终的sql
func (b *ConditionBuilder) Build() string {
	return strings.TrimSpace(strings.Join(b.wheres, " AND "))
}

// 清空
func (b *ConditionBuilder) Clear() {
	b.wheres = nil
}

// 添加多个查询AND条件
func (b *ConditionBuilder) Where(strs ...string) *ConditionBuilder {
	for _, str := range strs {
		if str != "" {
			// tip: 括号包裹条件，防止条件之间相互影响优先级
			b.wheres = append(b.wheres, "("+str+")")
		}
	}
	return b
}

func (b *ConditionBuilder) WhereMap(where map[string]interface{}) *ConditionBuilder {
	for k, v := range where {
		b.Equal(k, v)
	}
	return b
}

func (b *ConditionBuilder) TryMap(where map[string]interface{}) *ConditionBuilder {
	for k, v := range where {
		b.TryEqual(k, v)
	}
	return b
}

//添加多个OR条件
func (b *ConditionBuilder) Or(strs ...string) *ConditionBuilder {
	var cons []string
	for _, str := range strs {
		if str != "" {
			cons = append(cons, "("+str+")")
		}
	}
	if len(cons) > 0 {
		b.Where(strings.Join(cons, " OR "))
	}
	return b
}

// 添加相等条件
func (b *ConditionBuilder) Equal(dbField string, value interface{}) *ConditionBuilder {
	if value == nil {
		return b.Where(fmt.Sprintf("%s IS NULL", dbField))
	}
	return b.Where(fmt.Sprintf("%s = %s", dbField, ToString(value)))
}

// 添加相等条件，value为零值时跳过
func (b *ConditionBuilder) TryEqual(dbField string, value interface{}) *ConditionBuilder {
	if isEmpty(value) {
		return b
	}
	return b.Equal(dbField, value)
}

// 添加LIKE条件，左右模糊匹配，
// 如果需要单边模糊匹配，请使用Where
func (b *ConditionBuilder) Like(dbField, value string) *ConditionBuilder {
	return b.Where(fmt.Sprintf("%s LIKE %s", dbField, String("%"+value+"%")))
}

// 添加LIKE条件，左右模糊匹配，value为零值时跳过
func (b *ConditionBuilder) TryLike(dbField string, value string) *ConditionBuilder {
	if value := strings.TrimSpace(value); value != "" {
		return b.Like(dbField, value)
	}
	return b
}

// 添加多个LIKE条件
func (b *ConditionBuilder) MultiLike(dbFields []string, value string) *ConditionBuilder {
	v := "%" + String(value) + "%"
	var cons []string
	for _, field := range dbFields {
		cons = append(cons, fmt.Sprintf("%s LIKE %s", field, v))
	}
	return b.Or(cons...)
}

// 添加多个LIKE条件，value为零值时跳过
func (b *ConditionBuilder) TryMultiLike(dbFields []string, value string) *ConditionBuilder {
	if v := strings.TrimSpace(value); v != "" {
		return b.MultiLike(dbFields, v)
	}
	return b
}

// 添加BETWEEN条件
func (b *ConditionBuilder) Between(
	dbField string, start, end interface{}) *ConditionBuilder {
	return b.Where(fmt.Sprintf("%s BETWEEN %s AND %s",
		dbField, ToString(start), ToString(end)))
}

// 添加IN条件
func (b *ConditionBuilder) In(dbField string, values interface{}) *ConditionBuilder {
	if condition := buildInCondition(dbField, values); condition != "" {
		return b.Where(condition)
	}
	return b.Where("1=0")
}

// 添加IN条件，value为零值时跳过
func (b *ConditionBuilder) TryIn(dbField string, values interface{}) *ConditionBuilder {
	if condition := buildInCondition(dbField, values); condition != "" {
		return b.Where(condition)
	}
	return b
}

// 添加NOT IN条件
func (b *ConditionBuilder) NotIn(dbField string, values interface{}) *ConditionBuilder {
	if condition := buildNotInCondition(dbField, values); condition != "" {
		return b.Where(condition)
	}
	return b
}

// 添加Any条件
// structValues 可传类型：
// 		string: 子查询sql
// 		array/slice: 结果集，效果同In
func (b *ConditionBuilder) Any(dbField string, values interface{}) *ConditionBuilder {
	if condition := buildAnyCondition(dbField, values); condition != "" {
		return b.Where(condition)
	}
	return b.Where("1=0")
}

// 添加IN条件，value为零值时跳过
func (b *ConditionBuilder) TryAny(dbField string, values interface{}) *ConditionBuilder {
	if condition := buildAnyCondition(dbField, values); condition != "" {
		return b.Where(condition)
	}
	return b
}

// 添加时间范围条件，value为零值时跳过
func (b *ConditionBuilder) TryTimeRange(
	dbField string, startTime, endTime time.Time) *ConditionBuilder {
	if !startTime.IsZero() && !endTime.IsZero() {
		return b.Between(dbField, startTime, endTime)
	}
	if !startTime.IsZero() {
		return b.Where(fmt.Sprintf("%s >= %s", dbField, ToString(startTime)))
	}
	if !endTime.IsZero() {
		return b.Where(fmt.Sprintf("%s <= %s", dbField, ToString(endTime)))
	}
	return b
}

// 添加日期范围条件，value为零值时跳过
func (b *ConditionBuilder) TryDateRange(
	dbField string, startDate, endDate time.Time) *ConditionBuilder {
	if !startDate.IsZero() {
		startDate, _ = time.Parse(TimeLayout, startDate.Format(DateLayout)+" 00:00:00")
	}
	if !endDate.IsZero() {
		endDate, _ = time.Parse(TimeLayout, endDate.Format(DateLayout)+" 23:59:59")
	}
	return b.TryTimeRange(dbField, startDate, endDate)
}

func buildInCondition(field string, values interface{}) string {
	if v := sliceValue(values); v != "" {
		return fmt.Sprintf("%s IN (%s)", field, v)
	}
	return ""
}
func buildNotInCondition(field string, values interface{}) string {
	if v := sliceValue(values); v != "" {
		return fmt.Sprintf("%s NOT IN (%s)", field, v)
	}
	return ""
}

func buildAnyCondition(field string, values interface{}) string {
	switch values.(type) {
	case string:
		if values == "" {
			return ""
		}
		return fmt.Sprintf("%s = ANY(%s)", field, values)
	default:
		if v := sliceValue(values); v != "" {
			return fmt.Sprintf("%s = ANY(ARRAY[%s])", field, v)
		}
		return ""
	}
}
