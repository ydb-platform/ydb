LIBRARY()

SRCS(
    kqp_compile_service_helpers.cpp
)

PEERDIR(
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(ut)
