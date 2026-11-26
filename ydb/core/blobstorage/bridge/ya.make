LIBRARY()
    SRCS(keep_check.cpp)
    PEERDIR(ydb/core/base)
END()

RECURSE(
    proxy
)
