LIBRARY()

SRCS(
    registry.cpp
    program.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/formats/arrow/protos
    ydb/core/tablet_flat
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/core/arrow_kernels/registry
)

YQL_LAST_ABI_VERSION()

END()
