GO_TEST_FOR(vendor/github.com/stretchr/testify/assert)

LICENSE(MIT)

DATA(arcadia/vendor/github.com/stretchr/testify)

TEST_CWD(vendor/github.com/stretchr/testify/assert)

GO_SKIP_TESTS(
    TestDirExists
    TestFileExists
    TestNoDirExists
    TestNoFileExists
    TestDidPanic
    TestPanicsWithValue
)

TAG(ya:go_total_report)

END()
