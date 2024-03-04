LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/simple_builder
    ydb/core/formats/arrow/switch
    library/cpp/actors/core
)

SRCS(
    read_filter_merger.cpp
)

END()
