LIBRARY()

SRCS(
    array.cpp
    date.cpp
    enum.cpp
    factory.cpp
    nullable.cpp
    numeric.cpp
    string.cpp
    tuple.cpp
)

PEERDIR(
    library/cpp/clickhouse/client/base
    library/cpp/clickhouse/client/types
)

END()
