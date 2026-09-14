PROGRAM(event_serialize_bench)

SRCS(
    bench.proto
    main.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/dnsresolver
    ydb/library/actors/interconnect
    library/cpp/monlib/dynamic_counters
)

END()
