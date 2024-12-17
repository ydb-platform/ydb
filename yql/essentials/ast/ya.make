LIBRARY()

SRCS(
    yql_ast.cpp
    yql_ast.h
    yql_constraint.cpp
    yql_constraint.h
    yql_ast_annotation.cpp
    yql_ast_annotation.h
    yql_ast_escaping.cpp
    yql_ast_escaping.h
    yql_errors.cpp
    yql_errors.h
    yql_expr.cpp
    yql_expr.h
    yql_expr_builder.cpp
    yql_expr_builder.h
    yql_expr_types.cpp
    yql_expr_types.h
    yql_gc_nodes.cpp
    yql_gc_nodes.h
    yql_type_string.cpp
    yql_type_string.h
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/colorizer
    library/cpp/containers/sorted_vector
    library/cpp/containers/stack_vector
    library/cpp/deprecated/enum_codegen
    library/cpp/enumbitset
    library/cpp/string_utils/levenshtein_diff
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/utils/fetch
    yql/essentials/core/issue
    yql/essentials/core/url_lister/interface
    yql/essentials/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
