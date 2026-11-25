YQL_LIBRARY()

PROVIDES(
    yql_pg_sql_translator
    yql_pg_runtime
)

PEERDIR(
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/minikql
    yql/essentials/sql/settings
)

SRCS(
    pg_sql_dummy.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    yql/udfs/kikimr
    rtmapreduce/tools/shmdump
    cloud/filestore/libs/storage/core
    cloud/filestore/libs/storage/perf
    cloud/storage/core/libs/kikimr
    cloud/blockstore/libs/storage/core
    logbroker/pipe-parser/ytsender-ng
)

END()
