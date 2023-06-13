LIBRARY()

ADDINCL(
    contrib/libs/grpc
    contrib/libs/grpc/include
)

SRCS(
    backend.cpp
    backend_creator.cpp
    client_impl.cpp
    counters.cpp
    helpers.cpp
    grpc_io.cpp
    grpc_status_code.cpp
    clock.cpp
    duration_counter.cpp
    logger.cpp
    throttling.cpp
    proto_weighing.cpp
    GLOBAL registrar.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/logger/global
    library/cpp/threading/future
    library/cpp/monlib/dynamic_counters
    library/cpp/unified_agent_client/proto
)

GENERATE_ENUM_SERIALIZATION(grpc_io.h)

END()

RECURSE(
    examples
)
