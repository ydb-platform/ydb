UNITTEST_FOR(ydb/services/fq)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    ut_utils.cpp
    fq_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/core/yq/libs/control_plane_storage
    ydb/core/yq/libs/db_schema
    ydb/core/yq/libs/private_client
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/udfs/common/clickhouse/client
    ydb/library/yql/utils
    ydb/public/lib/fq
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:14)

END()
