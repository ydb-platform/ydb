UNITTEST_FOR(ydb/services/nbs)

SRCDIR(
    ydb/core/nbs/cloud/blockstore/libs/nbs_frontend
)

SRCS(
    classic_grpc_service_ut.cpp
    frontend_test.cpp
)

PEERDIR(
    ydb/library/actors/testlib
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/grpc
    ydb/core/nbs/cloud/blockstore/libs/nbs_frontend
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
