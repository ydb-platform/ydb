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
    ydb/library/yql/ast
    ydb/library/yql/minikql
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/core/issue
    ydb/library/yql/core/issue/protos
    ydb/library/yql/core/sql_types
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/parser/pg_wrapper/interface
    library/cpp/yson/node
)

YQL_LAST_ABI_VERSION()

END()
