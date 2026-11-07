LIBRARY()

SRCS(
    abstract.cpp
    events.cpp
    config.cpp
    service.cpp
)

PEERDIR(
    ydb/core/formats/arrow/reader/stats
    ydb/core/protos
    ydb/core/tx/general_cache/source
)

END()
