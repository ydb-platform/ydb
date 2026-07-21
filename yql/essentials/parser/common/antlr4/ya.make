LIBRARY()

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
    yql/essentials/parser/common
)

SRCS(
    depth_limiting_listener.cpp
    error_listener.cpp
)

END()
