LIBRARY()

OWNER(
    cthulhu
    g:kikimr
)

PEERDIR(
    library/cpp/lwtrace
)

SRCS(
    probes.cpp
    probes.h
)

END()
