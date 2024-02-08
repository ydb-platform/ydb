PROGRAM(example_02_discovery)

ALLOCATOR(LF)

SRCS(
    endpoint.cpp
    lookup.cpp
    main.cpp
    publish.cpp
    replica.cpp
    services.h
)

SRCS(
    protocol.proto
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/dnsresolver
    ydb/library/actors/interconnect
    ydb/library/actors/http
)

END()
