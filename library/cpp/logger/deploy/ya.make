LIBRARY()

PEERDIR(
    library/cpp/json/writer
    library/cpp/logger
)

SRCS(
    backend.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
