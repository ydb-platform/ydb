LIBRARY()

PEERDIR(
    library/cpp/logger
)

SRCS(
    common.cpp
    global.cpp
    rty_formater.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    contrib/libs/leveldb
)

END()

RECURSE_FOR_TESTS(
    ut
)
