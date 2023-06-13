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
    library/cpp/actors/core
    library/cpp/actors/dnsresolver
    library/cpp/actors/interconnect
    library/cpp/actors/http
)

END()
