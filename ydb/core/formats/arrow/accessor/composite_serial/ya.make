LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/formats/arrow/accessor/abstract
    ydb/core/formats/arrow/common
    ydb/core/formats/arrow/save_load
)

SRCS(
    accessor.cpp
)

END()
