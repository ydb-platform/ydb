LIBRARY()

SRCS(
    assign.cpp
    common.cpp
    encode.cpp
    http_url.h
    location.cpp
    other.cpp
    parse.cpp
    qargs.cpp
    uri.cpp
    encodefsm.rl6
    parsefsm.rl6
)

PEERDIR(
    contrib/libs/libidn
    library/cpp/charset
)

END()

RECURSE(
    benchmark
    ut
)
