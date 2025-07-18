LIBRARY()

SRCS(
    actor.cpp
    fetcher.cpp
    fetching_executor.cpp
    fetching_steps.cpp
    contexts.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/library/actors/core
    ydb/core/tx/columnshard/engines/writer
    ydb/core/tx/columnshard/export/events
    ydb/core/kqp/compute_actor
)

GENERATE_ENUM_SERIALIZATION(contexts.h)

END()
