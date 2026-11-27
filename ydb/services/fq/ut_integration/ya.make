UNITTEST_FOR(ydb/services/fq)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SUPPRESSIONS(tsan.supp)

SRCS(
    ut_utils.cpp
    fq_ut.cpp
)

PEERDIR(
    contrib/libs/curl
    library/cpp/json
    library/cpp/protobuf/json
    library/cpp/protobuf/util
    library/cpp/retry
    library/cpp/testing/common
    library/cpp/time_provider
    ydb/core/fq/libs/actors
    ydb/core/fq/libs/control_plane_storage
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/fq/libs/db_schema
    ydb/core/fq/libs/events
    ydb/core/fq/libs/mock
    ydb/core/fq/libs/private_client
    ydb/core/testlib/default
    ydb/library/actors/testlib
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/udfs/common/clickhouse/client
    ydb/public/lib/fq
    ydb/public/sdk/cpp/src/client/driver
    ydb/services/ydb
    yql/essentials/public/issue
    yql/essentials/utils/network
)

YQL_LAST_ABI_VERSION()

END()
