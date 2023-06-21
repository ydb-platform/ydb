LIBRARY()

PEERDIR(
    library/cpp/actors/core
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/minikql/computation/llvm
    ydb/library/yql/utils
)

SRCS(
    yql_common_dq_factory.cpp
)

YQL_LAST_ABI_VERSION()


END()

