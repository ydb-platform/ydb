LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    client.h
    connection.h
    transaction.h
    table_value_consumer.h
)

PEERDIR(
    library/cpp/testing/gtest_extensions
    yt/yt/build
    yt/yt/core
)

END()
