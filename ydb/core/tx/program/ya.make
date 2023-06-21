LIBRARY()

SRCS(
    registry.cpp
    program.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/library/yql/minikql/comp_nodes/llvm
    ydb/library/yql/core/arrow_kernels/registry
)

YQL_LAST_ABI_VERSION()

END()
