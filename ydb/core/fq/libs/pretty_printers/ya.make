LIBRARY()

SRCS(
    graph_params_printer.cpp
    minikql_program_printer.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/core/fq/libs/graph_params/proto
    ydb/library/protobuf_printer
    ydb/library/yql/minikql
    ydb/library/yql/providers/dq/api/protos
)

YQL_LAST_ABI_VERSION()

END()
