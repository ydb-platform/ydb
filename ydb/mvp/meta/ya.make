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
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/providers/result/expr_nodes
    ydb/library/yql/core/expr_nodes
    ydb/library/aclib/protos
    library/cpp/protobuf/json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bin
)
