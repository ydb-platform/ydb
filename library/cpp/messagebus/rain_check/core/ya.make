LIBRARY()

PEERDIR(
    library/cpp/coroutine/engine
    library/cpp/deprecated/enum_codegen
    library/cpp/messagebus
    library/cpp/messagebus/actor
    library/cpp/messagebus/scheduler
)

SRCS(
    coro.cpp
    coro_stack.cpp
    env.cpp
    rain_check.cpp
    simple.cpp
    sleep.cpp
    spawn.cpp
    task.cpp
    track.cpp
)

END()
