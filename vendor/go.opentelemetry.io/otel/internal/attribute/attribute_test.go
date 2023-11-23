// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package attribute

import (
	"reflect"
	"testing"
)

var wrapFloat64SliceValue = func(v interface{}) interface{} {
	if vi, ok := v.([]float64); ok {
		return Float64SliceValue(vi)
	}
	return nil
}

var wrapInt64SliceValue = func(v interface{}) interface{} {
	if vi, ok := v.([]int64); ok {
		return Int64SliceValue(vi)
	}
	return nil
}

var wrapBoolSliceValue = func(v interface{}) interface{} {
	if vi, ok := v.([]bool); ok {
		return BoolSliceValue(vi)
	}
	return nil
}
var wrapStringSliceValue = func(v interface{}) interface{} {
	if vi, ok := v.([]string); ok {
		return StringSliceValue(vi)
	}
	return nil
}
var wrapAsBoolSlice = func(v interface{}) interface{} { return AsBoolSlice(v) }
var wrapAsInt64Slice = func(v interface{}) interface{} { return AsInt64Slice(v) }
var wrapAsFloat64Slice = func(v interface{}) interface{} { return AsFloat64Slice(v) }
var wrapAsStringSlice = func(v interface{}) interface{} { return AsStringSlice(v) }

func TestSliceValue(t *testing.T) {
	type args struct {
		v interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
		fn   func(interface{}) interface{}
	}{
		{
			name: "Float64SliceValue() two items",
			args: args{v: []float64{1, 2.3}}, want: [2]float64{1, 2.3}, fn: wrapFloat64SliceValue,
		},
		{
			name: "Int64SliceValue() two items",
			args: args{[]int64{1, 2}}, want: [2]int64{1, 2}, fn: wrapInt64SliceValue,
		},
		{
			name: "BoolSliceValue() two items",
			args: args{v: []bool{true, false}}, want: [2]bool{true, false}, fn: wrapBoolSliceValue,
		},
		{
			name: "StringSliceValue() two items",
			args: args{[]string{"123", "2"}}, want: [2]string{"123", "2"}, fn: wrapStringSliceValue,
		},
		{
			name: "AsBoolSlice() two items",
			args: args{[2]bool{true, false}}, want: []bool{true, false}, fn: wrapAsBoolSlice,
		},
		{
			name: "AsInt64Slice() two items",
			args: args{[2]int64{1, 3}}, want: []int64{1, 3}, fn: wrapAsInt64Slice,
		},
		{
			name: "AsFloat64Slice() two items",
			args: args{[2]float64{1.2, 3.1}}, want: []float64{1.2, 3.1}, fn: wrapAsFloat64Slice,
		},
		{
			name: "AsStringSlice() two items",
			args: args{[2]string{"1234", "12"}}, want: []string{"1234", "12"}, fn: wrapAsStringSlice,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fn(tt.args.v); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
