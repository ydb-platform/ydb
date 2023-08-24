package xerrors

import (
	"testing"

	"github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors"
)

func TestErrorfFormattingWithoutError(t *testing.T) {
	constructor := func(t *testing.T) error {
		return Errorf("errorf: %s", "not an error")
	}
	expected := assertxerrors.Expectations{
		ExpectedS: "errorf: not an error",
		ExpectedV: "errorf: not an error",
		Frames: assertxerrors.NewStackTraceModeExpectation(`
errorf: not an error
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestErrorfFormattingWithoutError.func1
        library/go/core/xerrors/errorf_formatting_without_error_test.go:11
`,
		),
		Stacks: assertxerrors.NewStackTraceModeExpectation(`
errorf: not an error
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestErrorfFormattingWithoutError.func1
        library/go/core/xerrors/errorf_formatting_without_error_test.go:11
github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
`,
			3, 4, 5, 6,
		),
		StackThenFrames: assertxerrors.NewStackTraceModeExpectation(`
errorf: not an error
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestErrorfFormattingWithoutError.func1
        library/go/core/xerrors/errorf_formatting_without_error_test.go:11
github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
`,
			3, 4, 5, 6,
		),
		StackThenNothing: assertxerrors.NewStackTraceModeExpectation(`
errorf: not an error
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestErrorfFormattingWithoutError.func1
        library/go/core/xerrors/errorf_formatting_without_error_test.go:11
github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
`,
			3, 4, 5, 6,
		),
		Nothing: assertxerrors.NewStackTraceModeExpectation("errorf: not an error"),
	}
	assertxerrors.RunTestsPerMode(t, expected, constructor)
}
