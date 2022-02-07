LIBRARY()

OWNER(g:kikimr)

SRCS(
    dnsresolver.cpp
    dnsresolver_caching.cpp
    dnsresolver_ondemand.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/libs/c-ares
)

ADDINCL(contrib/libs/c-ares)

END()

RECURSE_FOR_TESTS(ut)
