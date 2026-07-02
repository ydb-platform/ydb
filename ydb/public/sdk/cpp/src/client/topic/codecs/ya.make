LIBRARY()

SRCS(
    GLOBAL codecs.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/kafka
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic
)

END()
