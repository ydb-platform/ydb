LIBRARY()

SRCS(
    probes.cpp
    startup_probes.cpp
    write_orbit.cpp
)

PEERDIR(
    library/cpp/lwtrace
    ydb/core/tx/data_events
)

END()
