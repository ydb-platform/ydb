LIBRARY()

SRCS(
    abstract.cpp
    zero_level.cpp
    common_level.cpp
    counters.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
