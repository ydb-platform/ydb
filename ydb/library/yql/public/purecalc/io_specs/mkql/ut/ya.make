UNITTEST()

SIZE(MEDIUM)

TIMEOUT(300)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/public/purecalc/common
    ydb/library/yql/public/purecalc/io_specs/mkql
    ydb/library/yql/public/purecalc/ut/lib
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_spec.cpp
)

END()
