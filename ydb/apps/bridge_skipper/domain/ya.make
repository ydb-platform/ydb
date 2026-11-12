LIBRARY()

SRCS(
    types.cpp
    view.cpp
    health.cpp
    policy.cpp
    mutation.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/time_provider
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
