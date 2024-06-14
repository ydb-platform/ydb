LIBRARY()

SRCS(
    sock64.h
    sock_config.h
    sock_impl.h
    sock_listener.cpp
    sock_listener.h
    sock_ssl.h
    sock_settings.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/protos
    ydb/core/base
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
#    ut
)
