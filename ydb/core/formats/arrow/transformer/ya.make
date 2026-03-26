LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow_next
    ydb/core/formats/arrow/dictionary
    ydb/library/formats/arrow/transformer
)

SRCS(
    dictionary.cpp
)

END()
