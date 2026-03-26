LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow_next
    library/cpp/threading/future
    ydb/library/yql/providers/common/http_gateway
)

SRCS(
    arrow_reader.cpp
)

END()