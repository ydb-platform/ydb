UNITTEST_FOR(ydb/core/kqp)

SIZE(MEDIUM)
FORK_SUBTESTS()

SRCS(
    kqp_ydb_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    ydb/core/grpc_services/local_rpc
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/core/security/certificate_check/test_utils
    ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    ydb/library/yql/providers/s3/actors
    ydb/public/api/grpc
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
