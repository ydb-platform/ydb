LIBRARY()

SRCS(
    task_scheduler.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
