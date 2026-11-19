LIBRARY()

SRCS(
    export_actor.cpp
)

PEERDIR(
    ydb/core/kqp/common/events
    ydb/core/tx/columnshard/bg_tasks/events
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/engines/writer
    ydb/core/tx/columnshard/export/session
    ydb/library/actors/core
    ydb/library/signals
)

GENERATE_ENUM_SERIALIZATION(export_actor.h)

END()

RECURSE_FOR_TESTS(
    ut
)
