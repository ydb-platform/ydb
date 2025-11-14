LIBRARY()

SRCS(
    arrow_inference.cpp
    config.cpp
)

PEERDIR(
    contrib/libs/apache/arrow_next
    ydb/public/api/protos
)

# Added because of library header contrib/libs/apache/arrow_next/cpp/src/arrow/util/value_parsing.h
CFLAGS(
    -Wno-unused-parameter
)

END() 