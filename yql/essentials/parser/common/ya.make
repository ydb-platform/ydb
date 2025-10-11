LIBRARY()

ENABLE(YQL_STYLE_CPP)

PEERDIR(
    yql/essentials/public/issue
    yql/essentials/core/issue
)

SRCS(
    error.cpp
)

END()

RECURSE(
    antlr4
)
