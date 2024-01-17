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
    ydb/library/actors/core
    ydb/library/actors/protos
    library/cpp/string_utils/base64
    ydb/core/base
    ydb/core/pgproxy/protos
    ydb/core/protos
    ydb/core/raw_socket
)

END()

RECURSE_FOR_TESTS(ut)
