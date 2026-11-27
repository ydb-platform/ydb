LIBRARY()

SRCS(
    meta_cluster_info.cpp
    meta.cpp
    meta_cache.cpp
    meta_database_statuses.cpp
    meta_health.cpp
    meta_settings.cpp
    meta_versions.cpp
    mvp.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/mvp/meta/support_links
    ydb/mvp/meta/protos
    ydb/public/api/client/yc_private/resourcemanager
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/providers/result/expr_nodes
    yql/essentials/core/expr_nodes
    ydb/library/aclib/protos
    library/cpp/protobuf/json
    library/cpp/getopt
    library/cpp/string_utils/quote
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bin
    support_links
)

RECURSE_FOR_TESTS(
    ut
)
