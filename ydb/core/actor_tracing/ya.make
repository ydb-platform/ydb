LIBRARY()

SRCS(
    tracing_service.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/trace_data
    ydb/library/services
    ydb/core/protos
    ydb/core/base
)

END()

RECURSE_FOR_TESTS(
    ut
)
