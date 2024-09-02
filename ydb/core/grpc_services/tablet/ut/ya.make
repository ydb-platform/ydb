UNITTEST_FOR(ydb/core/grpc_services/tablet)

SIZE(MEDIUM)

SRCS(
    rpc_change_schema_ut.cpp
    rpc_execute_mkql_ut.cpp
    rpc_restart_tablet_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/grpc_services/local_rpc
)

YQL_LAST_ABI_VERSION()

END()
