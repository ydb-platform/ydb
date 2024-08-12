LIBRARY()

SUBSCRIBER(g:kikimr)

SRCS(
    workload.cpp
    state.cpp
)

PEERDIR(
    ydb/library/accessor
    ydb/library/workload/abstract
    ydb/public/api/protos
)

GENERATE_ENUM_SERIALIZATION(workload.h)

END()

RECURSE_FOR_TESTS(
    ut
)
