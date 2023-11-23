package assertpb

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

type TestingT interface {
	Errorf(format string, args ...interface{})
	FailNow()
	Helper()
}

func Equal(t TestingT, expected, actual interface{}, msgAndArgs ...interface{}) bool {
	t.Helper()

	if cmp.Equal(expected, actual, cmp.Comparer(proto.Equal)) {
		return true
	}

	diff := cmp.Diff(expected, actual, cmp.Comparer(proto.Equal))
	return assert.Fail(t, fmt.Sprintf("Not equal: \n"+
		"expected: %s\n"+
		"actual  : %s\n"+
		"diff    : %s", expected, actual, diff), msgAndArgs)
}

func Equalf(t TestingT, expected, actual interface{}, msg string, args ...interface{}) bool {
	t.Helper()

	return Equal(t, expected, actual, append([]interface{}{msg}, args...)...)
}
