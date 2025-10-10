LIBRARY()

ENABLE(YQL_STYLE_CPP)

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
    yql/essentials/parser/common
)

SRCS(
    error_listener.cpp
)

END()
