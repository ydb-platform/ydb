LIBRARY()

SRCS(
    abstract.cpp
    counters.cpp
    one_layer.cpp
    zero_level.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
