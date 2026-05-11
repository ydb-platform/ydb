LIBRARY()

SRCS(
    op_inspect.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/schemeshard
    ydb/core/tx/schemeshard/generated
)

END()
