LIBRARY()

SRCS(
    registry.cpp
    program.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/library/yql/minikql/comp_nodes
    ydb/core/formats/arrow/kernels/registry
)

YQL_LAST_ABI_VERSION()

END()
