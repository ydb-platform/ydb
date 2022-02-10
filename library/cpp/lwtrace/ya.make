LIBRARY()

OWNER(serxa)

PEERDIR(
    library/cpp/lwtrace/protos
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

RECURSE(mon)

RECURSE_FOR_TESTS(ut)
