LIBRARY()

SRCS(
    equeue.h
    equeue.cpp
    fast.h
)

PEERDIR(
    library/cpp/deprecated/atomic
    library/cpp/threading/bounded_queue
)

IF (NOT OS_ANDROID AND NOT ARCH_ARM)
    PEERDIR(
        library/cpp/yt/threading
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)

