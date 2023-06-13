UNITTEST()

PEERDIR(
    library/cpp/messagebus/actor
)

SRCS(
    ../actor_ut.cpp
    ../ring_buffer_ut.cpp
    ../tasks_ut.cpp
    ../what_thread_does_guard_ut.cpp
)

END()
