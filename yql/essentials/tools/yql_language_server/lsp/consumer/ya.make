LIBRARY()

PEERDIR(
    library/cpp/threading/blocking_queue
)

SRCS(
    base.cpp
    blocking_queue.cpp
    map.cpp
    parallel.cpp
    println.cpp
    rw_binary_semaphore.cpp
    tee.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
