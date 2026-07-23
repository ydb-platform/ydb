LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/control/lib
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/changes/abstract
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/writer
    ydb/core/tx/data_events
    ydb/core/tx/general_cache/source
    ydb/core/tx/limiter/grouped_memory/usage
    ydb/core/tx/priorities/usage
    ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()
