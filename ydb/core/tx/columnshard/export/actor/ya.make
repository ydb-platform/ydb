LIBRARY()

SRCS(
    export_actor.cpp
)

PEERDIR(
    ydb/core/kqp/compute_actor/events
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/bg_tasks/events
    ydb/core/tx/columnshard/export/session
    ydb/core/tx/columnshard/engines/writer
    ydb/library/actors/core
    ydb/library/signals
)

END()

RECURSE_FOR_TESTS(
    ut
)
