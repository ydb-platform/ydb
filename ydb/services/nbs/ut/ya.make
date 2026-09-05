UNITTEST_FOR(ydb/services/nbs)

SRCS(
    classic_grpc_service_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/nbs_frontend
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
