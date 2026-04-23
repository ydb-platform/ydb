PROGRAM(flat_v2_sections_probe)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/dnsresolver
    ydb/library/actors/interconnect
    library/cpp/monlib/dynamic_counters
)

END()
