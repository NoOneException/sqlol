package sqlol

import (
	"reflect"
	"testing"
)

func TestStructExportedFields(t *testing.T) {
	type TestOne struct {
		Name string
	}
	type TestTwo struct {
		TestOne
		NameTwo string
	}
	type TestThree struct {
		TestTwo
		NameThree string
	}
	type TestFour struct {
		TagName string `sql:"Tagname"`
	}
	type args struct {
		obj reflect.Type
	}
	tests := []struct {
		name       string
		args       args
		wantFields []string
	}{
		{
			name: `one`,
			args: args{
				obj: reflect.TypeOf(TestOne{}),
			},
			wantFields: []string{`Name`},
		},
		{
			name: `two`,
			args: args{
				obj: reflect.TypeOf(TestTwo{}),
			},
			wantFields: []string{`Name`, `NameTwo`},
		},
		{
			name: `three`,
			args: args{
				obj: reflect.TypeOf(TestThree{}),
			},
			wantFields: []string{`Name`, `NameTwo`, `NameThree`},
		},
		{
			name: `four`,
			args: args{
				obj: reflect.TypeOf(TestFour{}),
			},
			wantFields: []string{`Tagname`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotFields := structExportedFields(tt.args.obj); !reflect.DeepEqual(gotFields, tt.wantFields) {
				t.Errorf("structFields() = %v, want %v", gotFields, tt.wantFields)
			}
		})
	}
}
