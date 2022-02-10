OWNER(
    pg
    mvel
    g:util
    g:base
)

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

END()
