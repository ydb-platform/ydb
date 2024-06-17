LIBRARY()

SRCS(
    saver.cpp
    index_info.cpp
    loader.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/formats/arrow/transformer
    ydb/core/formats/arrow/serializer
)

YQL_LAST_ABI_VERSION()

END()
