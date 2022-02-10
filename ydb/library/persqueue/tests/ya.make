LIBRARY()

OWNER(g:logbroker)

SRCS(counters.cpp)


PEERDIR(
    library/cpp/testing/unittest
    library/cpp/http/io
    library/cpp/json
)

END()
