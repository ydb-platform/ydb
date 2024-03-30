LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/simple_builder
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
)

SRCS(
    merger.cpp
    position.cpp
    heap.cpp
    result_builder.cpp
)

END()
