LIBRARY()

SRCS(
    registry.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
)

YQL_LAST_ABI_VERSION()

END()
