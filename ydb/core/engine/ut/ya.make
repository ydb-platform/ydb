UNITTEST_FOR(ydb/core/engine)

ALLOCATOR(J)

FORK_SUBTESTS()

TIMEOUT(150)

SIZE(MEDIUM)

SRCS(
    mkql_engine_flat_host_ut.cpp
    mkql_engine_flat_ut.cpp
    kikimr_program_builder_ut.cpp
    mkql_proto_ut.cpp
)

PEERDIR(
    ydb/core/engine/minikql
    ydb/core/kqp/ut/common
    ydb/core/tablet_flat/test/libs/table
    ydb/library/mkql_proto/ut/helpers
    ydb/library/yql/public/udf/service/exception_policy
    #ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
