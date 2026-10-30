LIBRARY()

SRCS(
    catalog.cpp
    connection.cpp
    listener.cpp
    protocol.cpp
)

PEERDIR(
    library/cpp/containers/absl
    ydb/core/base
    ydb/core/raw_socket
    ydb/core/tx/scheme_cache
    library/cpp/svnversion
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/services
    ydb/services/sql/session
)

END()

RECURSE_FOR_TESTS(
    ut
)
