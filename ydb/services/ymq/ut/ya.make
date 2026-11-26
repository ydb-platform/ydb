UNITTEST_FOR(ydb/services/ymq)

SIZE(SMALL)

SRCS(
    rpc_params_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
