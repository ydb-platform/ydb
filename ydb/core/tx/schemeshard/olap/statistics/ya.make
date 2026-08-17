LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/schemeshard/olap/common
)

END()
