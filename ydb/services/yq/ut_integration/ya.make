UNITTEST_FOR(ydb/services/yq) 

OWNER( 
    g:kikimr 
    g:yq 
    g:yql 
) 
 
FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    ut_utils.cpp
    yq_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib 
    ydb/core/yq/libs/control_plane_storage
    ydb/core/yq/libs/db_resolver 
    ydb/core/yq/libs/db_schema 
    ydb/core/yq/libs/private_client 
    ydb/services/ydb 
    ydb/services/yq 
    ydb/library/yql/udfs/common/clickhouse/client
    ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:14)

END()
