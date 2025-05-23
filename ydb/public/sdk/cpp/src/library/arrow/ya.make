RECURSE(
    serializer
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/public/sdk/cpp/src/library/arrow/serializer
    ydb/library/formats/arrow
)

SRCS(
    arrow_helpers.cpp
)

END()
