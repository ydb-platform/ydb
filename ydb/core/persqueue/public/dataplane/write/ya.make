LIBRARY()

SRCS(
    deferred_destination_upsert_actor.cpp
    events.cpp
    partition_writer.cpp
    partition_writer_cache_actor.cpp
    write_request_info.cpp
    write_session_logic_actor.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/jaeger_tracing
    ydb/core/persqueue/common
    ydb/core/persqueue/deferred_publish
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/persqueue/public/codecs
    ydb/core/persqueue/public/counters
    ydb/core/persqueue/public/describer
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/core/tx/scheme_cache
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/persqueue/topic_parser
    ydb/library/wilson_ids
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
