UNITTEST_FOR(library/cpp/actors/dnsresolver)

PEERDIR(
    library/cpp/actors/testlib
)

SRCS(
    dnsresolver_caching_ut.cpp
    dnsresolver_ondemand_ut.cpp
    dnsresolver_ut.cpp
)

ADDINCL(contrib/libs/c-ares/include)

TAG(ya:external)
REQUIREMENTS(network:full)

END()
