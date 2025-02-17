LIBRARY()

SRCS(
    type_ann_blocks.cpp
    type_ann_blocks.h
    type_ann_columnorder.cpp
    type_ann_columnorder.h
    type_ann_core.cpp
    type_ann_core.h
    type_ann_expr.cpp
    type_ann_expr.h
    type_ann_impl.h
    type_ann_join.cpp
    type_ann_list.cpp
    type_ann_list.h
    type_ann_pg.cpp
    type_ann_pg.h
    type_ann_types.cpp
    type_ann_types.h
    type_ann_wide.cpp
    type_ann_wide.h
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
    yql/essentials/parser/pg_catalog
    yql/essentials/core/sql_types
    yql/essentials/parser/pg_wrapper/interface
    library/cpp/yson/node
)

YQL_LAST_ABI_VERSION()

END()
