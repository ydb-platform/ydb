LIBRARY()
    SRCS(keep_check.cpp)
    PEERDIR(
        ydb/core/base
        ydb/library/actors/async
    )
END()

RECURSE(
    proxy
)
