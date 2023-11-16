GO_TEST_FOR(vendor/github.com/klauspost/compress/zstd)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SIZE(MEDIUM)

TAG(ya:go_total_report)

DATA(
    arcadia/vendor/github.com/klauspost/compress/testdata
    arcadia/vendor/github.com/klauspost/compress/zstd/testdata
)

TEST_CWD(vendor/github.com/klauspost/compress/zstd)

REQUIREMENTS(ram:13)

END()
