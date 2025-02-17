LIBRARY()

SRCS(
    collection.cpp
    predicate_node.cpp
    settings.cpp
    type_ann.cpp
    physical_opt.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/core/expr_nodes_gen
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
