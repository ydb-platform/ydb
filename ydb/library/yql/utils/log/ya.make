LIBRARY()

OWNER(
    udovichenko-r
    g:yql
)

SRCS(
    context.cpp
    log.cpp
    profile.cpp
    tls_backend.cpp
)

PEERDIR(
    library/cpp/logger
    library/cpp/logger/global
)

END()

RECURSE_FOR_TESTS(
    ut
)
