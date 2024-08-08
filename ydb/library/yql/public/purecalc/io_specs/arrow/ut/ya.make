UNITTEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(300)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/public/purecalc
    ydb/library/yql/public/purecalc/io_specs/arrow
    ydb/library/yql/public/purecalc/ut/lib
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_spec.cpp
)

END()
