LIBRARY()

PEERDIR(
    ydb/library/yql/minikql/comp_nodes/llvm14
    library/cpp/testing/unittest
    contrib/libs/protobuf
)

YQL_LAST_ABI_VERSION()

SRCS(
    helpers.cpp
)

END()
