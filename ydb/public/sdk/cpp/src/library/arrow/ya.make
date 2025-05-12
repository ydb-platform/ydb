RECURSE(
    serializer
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/public/sdk/cpp/src/library/arrow/serializer
    ydb/library/formats/arrow/validation
)

SRCS(
    arrow_helpers.cpp
)

END()