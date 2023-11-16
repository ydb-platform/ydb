package errors

import (
	"fmt"
	"reflect"
	"testing"
)

func TestCause(t *testing.T) {
	err1 := fmt.Errorf("1")
	erra := Wrap(err1, "wrap 2")
	errb := Wrap(erra, "wrap3")

	v, ok := Cause(errb)
	if !ok {
		t.Error("unexpected false")
		return
	}
	if !reflect.DeepEqual(v, erra.(*wrapError).frame) {
		t.Errorf("want %+v, got %+v", v, erra.(*wrapError).frame)
	}
}
