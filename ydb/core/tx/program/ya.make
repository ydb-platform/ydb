LIBRARY()

SRCS(
    registry.cpp
    program.cpp
    builder.cpp
    resolver.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/formats/arrow/filter
    ydb/core/formats/arrow/printer
    ydb/core/formats/arrow/program
    ydb/core/protos
    ydb/library/formats/arrow/protos
    ydb/core/tablet_flat
    yql/essentials/minikql/comp_nodes
    yql/essentials/core/arrow_kernels/registry
)

YQL_LAST_ABI_VERSION()

END()
