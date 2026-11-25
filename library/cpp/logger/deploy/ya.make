LIBRARY()

PEERDIR(
    library/cpp/json/writer
    library/cpp/logger
)

SRCS(
    backend.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    contrib/libs/leveldb
)

END()

RECURSE_FOR_TESTS(
    ut
)
