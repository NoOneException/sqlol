package sqlol

import (
	"database/sql/driver"
	"encoding/json"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func CamelsToSnakes(fields []string) (result []string) {
	for _, field := range fields {
		result = append(result, CamelToSnake(field))
	}
	return
}

func CamelToSnake(str string) string {
	var slice []string
	start := 0
	for end, char := range str {
		if end+1 < len(str) {
			next := str[end+1]
			if char < 'A' || char > 'Z' {
				if next >= 'A' && next <= 'Z' { // 非大写下一个是大写
					slice = append(slice, str[start:end+1])
					start, end = end+1, end+1
				}
			} else if end+2 < len(str) && (next >= 'A' && next <= 'Z') {
				if next2 := str[end+2]; next2 < 'A' || next2 > 'Z' {
					slice = append(slice, str[start:end+1])
					start, end = end+1, end+1
				}
			}
		} else {
			slice = append(slice, str[start:end+1])
		}
	}
	return strings.ToLower(strings.Join(slice, "_"))
}

func SnakeToCamel(s string) string {
	words := strings.Split(s, "_")
	res := ``
	for _, v := range words {
		if len(v) == 0 {
			continue
		}
		if len(v) == 1 {
			res += strings.ToUpper(string(v[0]))
			continue
		}
		res += strings.ToUpper(string(v[0])) + v[1:]
	}
	return res
}

// esc
// For more details,refer to 4.1.2.1 String Constants on
// https://www.postgresql.org/docs/9.5/sql-syntax-lexical.html
func String(s string) string {
	s = strings.Replace(s, "'", "''", -1)
	s = strings.Replace(s, "\000", "", -1)
	return "'" + s + "'"
}

func ToString(i interface{}) string {
	// special types
	switch v := i.(type) {
	case []byte:
		return string(v)
	case time.Time:
		// postgres all time type has 1 microsecond resolution.
		return "'" + v.Format("2006-01-02T15:04:05.999999Z07:00") + "'"
	case driver.Valuer:
		return valuer(v)
	case nil:
		return "NULL"
	}

	// basic types: use kind to handle type redefine
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.String:
		return String(v.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Bool:
		if v.Bool() {
			return "true"
		} else {
			return "false"
		}
	case reflect.Float32:
		return strconv.FormatFloat(v.Float(), 'G', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'G', -1, 64)
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return "NULL"
		} else {
			return ToString(v.Elem().Interface())
		}
	}

	// other types: use json
	return JsonString(i)
}

func JsonString(data interface{}) string {
	b, err := json.Marshal(data)
	if err != nil {
		log.Panic("sqlol json.Marshal: ", err)
	}
	return String(string(b))
}

var valuerType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

func valuer(v driver.Valuer) string {
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr && rv.IsNil() &&
		rv.Type().Elem().Implements(valuerType) {
		return "NULL"
	}

	ifc, err := v.Value()
	if err != nil {
		log.Panic("sqlol valuer: ", err)
	}
	switch s := ifc.(type) {
	case string:
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return s
		} else {
			return String(s)
		}
	default:
		return ToString(ifc)
	}
}

func sliceValue(values interface{}) string {
	if values == nil {
		return ""
	}
	v := reflect.ValueOf(values)
	kind := v.Kind()
	if kind != reflect.Array && kind != reflect.Slice {
		return ""
	}
	vLen := v.Len()
	if vLen == 0 {
		return ""
	}
	var s []string
	for i := 0; i < vLen; i++ {
		s = append(s, ToString(v.Index(i).Interface()))
	}
	return strings.Join(s, ",")
}

func isEmpty(value interface{}) bool {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
}

func copyStringSlice(src []string) []string {
	res := make([]string, len(src))
	copy(res, src)
	return res
}

func StringSliceDiff(source, exclude []string) []string {
	excludeMap := make(map[string]bool)
	for _, v := range exclude {
		excludeMap[v] = true
	}
	var result []string
	for _, v := range source {
		if _, ok := excludeMap[v]; ok {
			result = append(result, v)
		}
	}
	return result
}

func StructExportedFields(obj interface{}) (fields []string) {
	return structExportedFields(reflect.TypeOf(obj))
}

func structExportedFields(t reflect.Type) (fields []string) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	numField := t.NumField()
	for i := 0; i < numField; i++ {
		field := t.Field(i)
		if field.Anonymous {
			fields = append(fields, structExportedFields(field.Type)...)
		} else {
			if field.Name[0] >= 'A' && field.Name[0] <= 'Z' {
				fieldName := field.Tag.Get(`sql`)
				if fieldName == "" {
					fieldName = field.Name
				}
				fields = append(fields, fieldName)
			}
		}
	}
	return
}

func StructValues(data interface{}, fields []string) string {
	value := reflect.ValueOf(data)
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		var slice []string
		for i := 0; i < value.Len(); i++ {
			slice = append(slice, structValues(value.Index(i), fields))
		}
		return strings.Join(slice, ",")
	default:
		return structValues(value, fields)
	}
}

func structValues(value reflect.Value, fields []string) string {
	if value.Kind() == reflect.Ptr || value.Kind() == reflect.Interface {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		log.Panic("bsql: data must be struct or struct slice.")
	}
	var slice []string
	for _, fieldName := range fields {
		field := structField(value, fieldName)
		if !field.IsValid() {
			log.Panic("bsql: no field '" + fieldName + "' in struct")
		}
		slice = append(slice, ToString(field.Interface()))
	}
	return "(" + strings.Join(slice, ",") + ")"
}

func structField(strct reflect.Value, fieldName string) reflect.Value {
	if strings.IndexByte(fieldName, '.') <= 0 {
		return strct.FieldByName(fieldName)
	}
	for _, name := range strings.Split(fieldName, ".") {
		strct = strct.FieldByName(name)
		if !strct.IsValid() {
			return strct
		}
	}
	return strct
}
