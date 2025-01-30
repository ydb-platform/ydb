RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    meta.cpp
    meta_cache.cpp
    meta_cache.h
    meta_cloud.h
    meta_cluster.h
    meta_clusters.h
    meta_cp_databases.h
    meta_cp_databases_verbose.h
    meta_db_clusters.h
    meta_versions.cpp
    meta_versions.h
    mvp.cpp
    mvp.h
)

PEERDIR(
    ydb/mvp/core
    ydb/public/api/client/yc_private/resourcemanager
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/providers/result/expr_nodes
    yql/essentials/core/expr_nodes
    ydb/library/aclib/protos
    library/cpp/protobuf/json
    library/cpp/getopt
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bin
)
