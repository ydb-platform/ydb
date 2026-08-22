LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/common
    ydb/core/formats/arrow/switch
    ydb/library/actors/core
    ydb/library/services
    ydb/library/formats/arrow/hash
)

SRCS(
    calcer.cpp
)

END()

