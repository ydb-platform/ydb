LIBRARY(messagebus_actor)

SRCS(
    executor.cpp
    thread_extra.cpp
    what_thread_does.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
