LIBRARY()

OWNER(
    vvvv
    g:yql
    g:yql_ydb_core
)

SRCS(
    type_ann_core.cpp
    type_ann_core.h
    type_ann_columnorder.cpp
    type_ann_columnorder.h
    type_ann_expr.cpp
    type_ann_expr.h
    type_ann_join.cpp
    type_ann_list.cpp
    type_ann_list.h
    type_ann_types.cpp
    type_ann_types.h
    type_ann_wide.cpp
    type_ann_wide.h
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
    ydb/library/yql/providers/common/schema/expr
)

YQL_LAST_ABI_VERSION()

END()
