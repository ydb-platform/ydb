package xerrors

import (
	"errors"
	"testing"

	"github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors"
)

func TestErrorfFormattingWithStdError(t *testing.T) {
	constructor := func(t *testing.T) error {
		err := errors.New("new")
		return Errorf("errorf: %w", err)
	}
	expected := assertxerrors.Expectations{
		ExpectedS: "errorf: new",
		ExpectedV: "errorf: new",
		Frames: assertxerrors.NewStackTraceModeExpectation(`
errorf:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestErrorfFormattingWithStdError.func1
        library/go/core/xerrors/errorf_formatting_with_std_error_test.go:13
new`,
		),
		Stacks: assertxerrors.NewStackTraceModeExpectation(`
errorf:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestErrorfFormattingWithStdError.func1
        library/go/core/xerrors/errorf_formatting_with_std_error_test.go:13
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
new`,
			3, 4, 5, 6,
		),
		StackThenFrames: assertxerrors.NewStackTraceModeExpectation(`
errorf:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestErrorfFormattingWithStdError.func1
        library/go/core/xerrors/errorf_formatting_with_std_error_test.go:13
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
new`,
			3, 4, 5, 6,
		),
		StackThenNothing: assertxerrors.NewStackTraceModeExpectation(`
errorf:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestErrorfFormattingWithStdError.func1
        library/go/core/xerrors/errorf_formatting_with_std_error_test.go:13
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
new`,
			3, 4, 5, 6,
		),
		Nothing: assertxerrors.NewStackTraceModeExpectation("errorf: new"),
	}
	assertxerrors.RunTestsPerMode(t, expected, constructor)
}
