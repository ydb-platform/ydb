UNITTEST_FOR(ydb/library/mkql_proto)

ALLOCATOR(J)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    mkql_proto_ut.cpp
)

PEERDIR(
    ydb/library/mkql_proto/ut/helpers
    yql/essentials/public/udf/service/exception_policy
    ydb/core/yql_testlib
)

YQL_LAST_ABI_VERSION()

END()
