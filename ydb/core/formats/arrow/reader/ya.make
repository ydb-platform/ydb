YQL_LIBRARY()

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
    batch_iterator.cpp  # gn: into ydb/core/formats/arrow
    merger.cpp  # gn: into ydb/core/formats/arrow
    position.cpp  # gn: into ydb/core/formats/arrow
    heap.cpp  # gn: into ydb/core/formats/arrow
    result_builder.cpp  # gn: into ydb/core/formats/arrow
)

END()
