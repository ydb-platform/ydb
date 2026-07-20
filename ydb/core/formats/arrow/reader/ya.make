LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/accessor/abstract
    ydb/core/formats/arrow/common
    ydb/core/formats/arrow/container
    ydb/core/formats/arrow/filter
    ydb/core/formats/arrow/switch
    ydb/core/scheme

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
