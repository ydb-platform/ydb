LIBRARY()

PEERDIR(
    library/cpp/lwtrace/protos
    library/cpp/deprecated/atomic
)

SRCS(
    check.cpp
    control.cpp
    custom_action.cpp
    kill_action.cpp
    log_shuttle.cpp
    perf.cpp
    probes.cpp
    shuttle.cpp
    sleep_action.cpp
    start.cpp
    stderr_writer.cpp
    symbol.cpp
    trace.cpp
)

END()

RECURSE(
    example1
    example2
    example3
    example4
    example5
    mon
    tests
    ut
)
