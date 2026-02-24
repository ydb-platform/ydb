LIBRARY()

ENABLE(SKIP_YQL_STYLE_CPP)

SRCS(
    type_ann_blocks.cpp
    type_ann_columnorder.cpp
    type_ann_core.cpp
    type_ann_dict.cpp
    type_ann_expr.cpp
    type_ann_impl.h
    type_ann_join.cpp
    type_ann_list.cpp
    type_ann_pg.cpp
    type_ann_sql.cpp
    type_ann_types.cpp
    type_ann_wide.cpp
    type_ann_yql.cpp
    type_ann_match_recognize.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/minikql
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/core/issue
    yql/essentials/core/issue/protos
    yql/essentials/core/sql_types
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/common/provider
    yql/essentials/parser/pg_catalog
    yql/essentials/core/sql_types
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/langver
    library/cpp/yson/node
)

YQL_LAST_ABI_VERSION()

END()
