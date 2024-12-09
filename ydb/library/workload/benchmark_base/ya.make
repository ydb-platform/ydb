LIBRARY()

SUBSCRIBER(g:kikimr)

SRCS(
    workload.cpp
    state.cpp
)

PEERDIR(
    ydb/library/accessor
    ydb/library/workload/abstract
    ydb/library/yaml_json
    ydb/public/api/protos
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(workload.h)

END()

RECURSE_FOR_TESTS(
    ut
)
