LIBRARY()

SRCS(
    executor.cpp
    queue.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/common
    library/cpp/coroutine/engine
    library/cpp/threading/future
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
