LIBRARY()

PEERDIR(
    library/cpp/actors/core
)

SRCS(
    await_callback.cpp
    await_callback.h
    task_actor.cpp
    task_actor.h
    task_group.cpp
    task_group.h
    task.cpp
    task.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
