UNITTEST_FOR(ydb/services/nbs)

SRCDIR(
    ydb/core/nbs/cloud/blockstore/libs/nbs_frontend
)

SRCS(
    classic_grpc_service_ut.cpp
    frontend_test.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/nbs/cloud/blockstore/libs/nbs_frontend
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/protos
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/grpc
    ydb/core/nbs/nbs1_compat_api/cloud/storage/core/protos
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/actors/testlib
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
