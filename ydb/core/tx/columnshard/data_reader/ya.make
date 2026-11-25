LIBRARY()

SRCS(
    actor.cpp
    fetcher.cpp
    fetching_executor.cpp
    fetching_steps.cpp
    contexts.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/engines/writer
    ydb/library/actors/core
)

GENERATE_ENUM_SERIALIZATION(contexts.h)

END()
