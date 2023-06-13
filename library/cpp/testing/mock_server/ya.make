LIBRARY()

PEERDIR(
    library/cpp/http/misc
    library/cpp/http/server
)

SRCS(
    server.cpp
)

END()

RECURSE_FOR_TESTS(ut)
