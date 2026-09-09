LIBRARY()

SRCS(
    probes.cpp
    write_orbit.cpp
)

PEERDIR(
    library/cpp/lwtrace
    ydb/core/tx/data_events
)

END()
