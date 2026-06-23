LIBRARY()

SRCS(
    GLOBAL codecs.cpp
)

PEERDIR(
    ydb/library/kafka
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic
)

END()
