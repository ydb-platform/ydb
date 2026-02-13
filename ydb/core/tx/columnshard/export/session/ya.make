LIBRARY()

SRCS(
    GLOBAL session.cpp
    cursor.cpp
    GLOBAL task.cpp
    GLOBAL control.cpp
)

PEERDIR(
    ydb/core/grpc_services/cancelation/protos
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/bg_tasks
    ydb/core/tx/columnshard/export/protos
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/tx/columnshard/data_sharing/protos
    yql/essentials/core/expr_nodes
)

GENERATE_ENUM_SERIALIZATION(session.h)

END()
