LIBRARY()

SRCS(
    export_actor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/library/actors/core
    ydb/core/tx/columnshard/engines/writer
    ydb/core/kqp/compute_actor
)

END()
