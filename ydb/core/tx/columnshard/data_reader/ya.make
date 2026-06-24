LIBRARY()

SRCS(
    actor.cpp
    fetcher.cpp
    fetching_executor.cpp
    fetching_steps.cpp
    contexts.cpp
)

PEERDIR(
    ydb/core/kqp/compute_actor/events
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/engines/writer
    ydb/library/actors/core
    ydb/library/yql/dq/actors
)

GENERATE_ENUM_SERIALIZATION(contexts.h)

END()
