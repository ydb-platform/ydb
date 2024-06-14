LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/simple_builder
    ydb/core/formats/arrow/switch
    ydb/core/formats/arrow/common
    ydb/library/actors/core
    ydb/library/services
)

SRCS(
    batch_iterator.cpp
    merger.cpp
    position.cpp
    heap.cpp
    result_builder.cpp
)

END()
