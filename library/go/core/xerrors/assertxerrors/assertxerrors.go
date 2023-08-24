package assertxerrors

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/core/xerrors/internal/modes"
	"github.com/ydb-platform/ydb/library/go/test/testhelpers"
)

func RunTestsPerMode(t *testing.T, expected Expectations, constructor func(t *testing.T) error) {
	for _, mode := range modes.KnownStackTraceModes() {
		t.Run(fmt.Sprintf("Mode%s", mode), func(t *testing.T) {
			modes.SetStackTraceMode(mode)
			err := constructor(t)
			expected.Assert(t, err)
		})
	}
}

type StackTraceModeExpectation struct {
	expectedPlusV string
	lines         []int
}

func NewStackTraceModeExpectation(plusv string, lines ...int) StackTraceModeExpectation {
	return StackTraceModeExpectation{expectedPlusV: plusv, lines: lines}
}

type Expectations struct {
	ExpectedS        string
	ExpectedV        string
	Frames           StackTraceModeExpectation
	Stacks           StackTraceModeExpectation
	StackThenFrames  StackTraceModeExpectation
	StackThenNothing StackTraceModeExpectation
	Nothing          StackTraceModeExpectation
}

func (e Expectations) Assert(t *testing.T, err error) {
	assert.Equal(t, e.ExpectedS, fmt.Sprintf("%s", err))
	assert.Equal(t, e.ExpectedV, fmt.Sprintf("%v", err))

	var expected StackTraceModeExpectation
	switch modes.GetStackTraceMode() {
	case modes.StackTraceModeFrames:
		expected = e.Frames
	case modes.StackTraceModeStacks:
		expected = e.Stacks
	case modes.StackTraceModeStackThenFrames:
		expected = e.StackThenFrames
	case modes.StackTraceModeStackThenNothing:
		expected = e.StackThenNothing
	case modes.StackTraceModeNothing:
		expected = e.Nothing
	}

	assertErrorOutput(t, expected, err)
}

func assertErrorOutput(t *testing.T, expected StackTraceModeExpectation, err error) {
	// Cut starting \n's if needed (we use `` notation with newlines for expected error messages)
	preparedExpected := strings.TrimPrefix(expected.expectedPlusV, "\n")
	actual := fmt.Sprintf("%+v", err)

	var e error
	preparedExpected, e = testhelpers.RemoveLines(preparedExpected, expected.lines...)
	if !assert.NoErrorf(t, e, "lines removal from expected:\n%s", preparedExpected) {
		t.Logf("initial expected:\n%s", expected.expectedPlusV)
		t.Logf("initial actual:\n%s", actual)
		return
	}

	preparedActual, e := testhelpers.RemoveLines(actual, expected.lines...)
	if !assert.NoErrorf(t, e, "lines removal from actual:\n%s", actual) {
		t.Logf("initial expected:\n%s", expected.expectedPlusV)
		t.Logf("initial actual:\n%s", actual)
		return
	}

	if !assert.Equal(t, preparedExpected, preparedActual) {
		t.Logf("initial expected:\n%s", expected.expectedPlusV)
		t.Logf("initial actual:\n%s", actual)
	}
}
