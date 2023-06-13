LIBRARY()

SRCS(
    yql_aggregate_expander.cpp
    yql_atom_enums.h
    yql_callable_transform.cpp
    yql_callable_transform.h
    yql_csv.cpp
    yql_csv.h
    yql_data_provider.h
    yql_execution.cpp
    yql_execution.h
    yql_expr_constraint.cpp
    yql_expr_constraint.h
    yql_expr_csee.cpp
    yql_expr_csee.h
    yql_expr_optimize.cpp
    yql_expr_optimize.h
    yql_expr_type_annotation.cpp
    yql_expr_type_annotation.h
    yql_gc_transformer.cpp
    yql_gc_transformer.h
    yql_graph_transformer.cpp
    yql_graph_transformer.h
    yql_holding_file_storage.cpp
    yql_holding_file_storage.h
    yql_join.cpp
    yql_join.h
    yql_library_compiler.cpp
    yql_opt_proposed_by_data.cpp
    yql_opt_proposed_by_data.h
    yql_opt_range.cpp
    yql_opt_range.h
    yql_opt_rewrite_io.cpp
    yql_opt_rewrite_io.h
    yql_opt_utils.cpp
    yql_opt_utils.h
    yql_opt_window.cpp
    yql_opt_window.h
    yql_type_annotation.cpp
    yql_type_annotation.h
    yql_type_helpers.cpp
    yql_type_helpers.h
    yql_udf_index.cpp
    yql_udf_index.h
    yql_udf_index_package_set.cpp
    yql_udf_index_package_set.h
    yql_udf_resolver.cpp
    yql_udf_resolver.h
    yql_user_data.cpp
    yql_user_data.h
    yql_user_data_storage.cpp
    yql_user_data_storage.h
)

PEERDIR(
    library/cpp/enumbitset
    library/cpp/random_provider
    library/cpp/threading/future
    library/cpp/time_provider
    library/cpp/yson
    library/cpp/yson/node
    ydb/library/yql/ast
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/sql_types
    ydb/library/yql/core/credentials
    ydb/library/yql/minikql
    ydb/library/yql/protos
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/tz
    ydb/library/yql/sql/settings
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/providers/common/proto
)

GENERATE_ENUM_SERIALIZATION(yql_data_provider.h)

GENERATE_ENUM_SERIALIZATION(yql_user_data.h)

GENERATE_ENUM_SERIALIZATION(yql_atom_enums.h)

GENERATE_ENUM_SERIALIZATION(yql_type_annotation.h)

YQL_LAST_ABI_VERSION()

END()
