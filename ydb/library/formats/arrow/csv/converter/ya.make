LIBRARY()

SRCS(
    csv_arrow.cpp
)

PEERDIR(
    contrib/libs/apache/arrow_next
    ydb/public/api/protos
)

END()
