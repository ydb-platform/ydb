UNITTEST_FOR(ydb/library/mkql_proto)

ALLOCATOR(J)

FORK_SUBTESTS()

TIMEOUT(150)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    mkql_proto_ut.cpp
)

PEERDIR(
    ydb/library/mkql_proto/ut/helpers
    ydb/library/yql/public/udf/service/exception_policy
    ydb/core/yql_testlib
)

YQL_LAST_ABI_VERSION()

END()
