LIBRARY()

SRCS(
    pg_connection.cpp
    pg_connection.h
    pg_listener.h
    pg_log_impl.h
    pg_log.h
    pg_proxy_events.h
    pg_proxy_types.cpp
    pg_proxy_types.h
    pg_proxy.cpp
    pg_proxy.h
    pg_stream.h
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/protos
    ydb/core/base
    ydb/core/protos
    ydb/core/raw_socket
)

END()

RECURSE_FOR_TESTS(ut)
