LIBRARY()

SRCS(
    GLOBAL codecs.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic
)

# Kafka batch codec depends on ydb/library/kafka -> library/cpp/digest/crc32c -> contrib/libs/crcutil,
# which does not compile with clang-cl on Windows. The codec is disabled on Windows (see codecs.cpp).
IF (NOT OS_WINDOWS)
PEERDIR(
    ydb/library/kafka
)
ENDIF()

END()
