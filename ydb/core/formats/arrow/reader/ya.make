LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/switch
    ydb/core/formats/arrow/common
    ydb/library/actors/core
    ydb/library/services
    ydb/library/formats/arrow
)

SRCS(
    batch_iterator.cpp
    merger.cpp
    position.cpp
    heap.cpp
    result_builder.cpp
)

END()
