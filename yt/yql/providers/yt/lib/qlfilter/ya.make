YQL_LIBRARY()

SRCS(
    qlfilter.h
    qlfilter_extract.cpp
    qlfilter_generate.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/utils
    yql/essentials/utils/log
    yt/yql/providers/yt/expr_nodes
)

END()

RECURSE_FOR_TESTS(
    ut
)
