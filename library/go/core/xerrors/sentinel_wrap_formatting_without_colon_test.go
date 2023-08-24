package xerrors

import (
	"testing"

	"github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors"
)

func TestSentinelWrapFormattingWithoutColon(t *testing.T) {
	constructor := func(t *testing.T) error {
		sentinel := NewSentinel("sntnl_wrapper")
		err := NewSentinel("sentinel")
		return sentinel.Wrap(err)
	}
	expected := assertxerrors.Expectations{
		ExpectedS: "sntnl_wrapper: sentinel",
		ExpectedV: "sntnl_wrapper: sentinel",
		Frames: assertxerrors.NewStackTraceModeExpectation(`
sntnl_wrapper:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapFormattingWithoutColon.func1
        library/go/core/xerrors/sentinel_wrap_formatting_without_colon_test.go:13
sentinel`,
		),
		Stacks: assertxerrors.NewStackTraceModeExpectation(`
sntnl_wrapper:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapFormattingWithoutColon.func1
        library/go/core/xerrors/sentinel_wrap_formatting_without_colon_test.go:13
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
sentinel`,
			3, 4, 5, 6,
		),
		StackThenFrames: assertxerrors.NewStackTraceModeExpectation(`
sntnl_wrapper:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapFormattingWithoutColon.func1
        library/go/core/xerrors/sentinel_wrap_formatting_without_colon_test.go:13
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
sentinel`,
			3, 4, 5, 6,
		),
		StackThenNothing: assertxerrors.NewStackTraceModeExpectation(`
sntnl_wrapper:
    github.com/ydb-platform/ydb/library/go/core/xerrors.TestSentinelWrapFormattingWithoutColon.func1
        library/go/core/xerrors/sentinel_wrap_formatting_without_colon_test.go:13
    github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/github.com/ydb-platform/ydb/library/go/core/xerrors/assertxerrors/assertxerrors.go:18
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
sentinel`,
			3, 4, 5, 6,
		),
		Nothing: assertxerrors.NewStackTraceModeExpectation("sntnl_wrapper: sentinel"),
	}
	assertxerrors.RunTestsPerMode(t, expected, constructor)
}
