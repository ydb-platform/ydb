LIBRARY()

PEERDIR(
    yql/essentials/minikql/comp_nodes/llvm16
    library/cpp/testing/unittest
    contrib/libs/protobuf
)

YQL_LAST_ABI_VERSION()

SRCS(
    helpers.cpp
)

END()
