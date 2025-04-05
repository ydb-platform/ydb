LIBRARY()

SRCS(
    column_family.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/formats/arrow/protos
    ydb/core/formats/arrow/serializer
)

END()
