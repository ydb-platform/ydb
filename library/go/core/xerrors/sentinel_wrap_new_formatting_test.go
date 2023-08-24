package xerrors

import (
	"testing"

	"github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors"
)

func TestSentinelWrapNewFormatting(t *testing.T) {
	constructor := func(t *testing.T) error {
		err := New("new")
		sentinel := NewSentinel("sentinel")
		return sentinel.Wrap(err)
	}
	expected := assertxerrors.Expectations{
		ExpectedS: "sentinel: new",
		ExpectedV: "sentinel: new",
		Frames: assertxerrors.NewStackTraceModeExpectation(`
sentinel:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapNewFormatting.func1
        library/go/core/xerrors/sentinel_wrap_new_formatting_test.go:13
new
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapNewFormatting.func1
        library/go/core/xerrors/sentinel_wrap_new_formatting_test.go:11
`,
		),
		Stacks: assertxerrors.NewStackTraceModeExpectation(`
sentinel:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapNewFormatting.func1
        library/go/core/xerrors/sentinel_wrap_new_formatting_test.go:13
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
new
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapNewFormatting.func1
        library/go/core/xerrors/sentinel_wrap_new_formatting_test.go:11
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
`,
			3, 4, 5, 6, 10, 11, 12, 13,
		),
		StackThenFrames: assertxerrors.NewStackTraceModeExpectation(`
sentinel:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapNewFormatting.func1
        library/go/core/xerrors/sentinel_wrap_new_formatting_test.go:13
new
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapNewFormatting.func1
        library/go/core/xerrors/sentinel_wrap_new_formatting_test.go:11
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
`,
			6, 7, 8, 9,
		),
		StackThenNothing: assertxerrors.NewStackTraceModeExpectation(`
sentinel: new
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapNewFormatting.func1
        library/go/core/xerrors/sentinel_wrap_new_formatting_test.go:11
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
`,
			3, 4, 5, 6,
		),
		Nothing: assertxerrors.NewStackTraceModeExpectation("sentinel: new"),
	}
	assertxerrors.RunTestsPerMode(t, expected, constructor)
}
