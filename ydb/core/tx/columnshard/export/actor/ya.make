LIBRARY()

SRCS(
    export_actor.cpp
    write.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/export/controller/abstract
    ydb/library/actors/core
    ydb/core/tx/columnshard/engines/writer
    ydb/core/tx/columnshard/export/events
    ydb/core/kqp/compute_actor
)

END()
