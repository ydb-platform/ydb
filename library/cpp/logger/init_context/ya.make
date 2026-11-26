LIBRARY()

PEERDIR(
    library/cpp/logger
    library/cpp/config
    library/cpp/yconf
)
SRCS(
    config.cpp
    yconf.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    contrib/libs/leveldb
)

END()
