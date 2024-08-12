LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/services/bg_tasks/abstract
    ydb/core/protos
    ydb/core/tx/schemeshard/olap/common
)

END()
