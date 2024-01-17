LIBRARY()

SRCS(
    block.cpp
    client.cpp
    query.cpp
)

PEERDIR(
    contrib/libs/lz4
    contrib/restricted/cityhash-1.0.2
    library/cpp/clickhouse/client/base
    library/cpp/clickhouse/client/columns
    library/cpp/clickhouse/client/types
    library/cpp/openssl/io
)

END()
