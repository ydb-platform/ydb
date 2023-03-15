LIBRARY()

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/minikql/computation
    ydb/library/yql/minikql/invoke_builtins
    library/cpp/testing/unittest
    contrib/libs/protobuf
)

YQL_LAST_ABI_VERSION()

SRCS(
    helpers.cpp
)

END()
