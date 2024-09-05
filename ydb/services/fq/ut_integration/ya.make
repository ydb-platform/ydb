UNITTEST_FOR(ydb/services/fq)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    ut_utils.cpp
    fq_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/fq/libs/control_plane_storage
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/fq/libs/db_schema
    ydb/core/fq/libs/private_client
    ydb/core/testlib/default
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/udfs/common/clickhouse/client
    ydb/library/yql/utils
    ydb/public/lib/fq
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:14)
ENDIF()

END()
