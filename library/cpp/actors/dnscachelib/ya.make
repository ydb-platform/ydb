LIBRARY()

OWNER(
    davenger
    fomichev
    serxa
    dimanne
    single
)

SRCS(
    dnscache.cpp
    dnscache.h
    probes.cpp 
    probes.h 
    timekeeper.h
)

PEERDIR(
    contrib/libs/c-ares
    library/cpp/lwtrace
)

END()
