LIBRARY()

SRCS(
    catalog.cpp
    connection.cpp
    listener.cpp
    protocol.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/raw_socket
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/services
    ydb/services/sql/ddl
    ydb/services/sql/parse
)

END()

RECURSE_FOR_TESTS(
    ut
)
