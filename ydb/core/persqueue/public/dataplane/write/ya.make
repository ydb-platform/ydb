LIBRARY()

SRCS(
    deferred_destination_upsert_actor.cpp
    partition_writer.cpp
    partition_writer_cache_actor.cpp
)

PEERDIR(
    ydb/core/persqueue/deferred_publish
    ydb/core/persqueue/events
    ydb/core/persqueue/writer
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
