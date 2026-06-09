LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    json_data_generator.cpp
    json_workload_generator.cpp
    json_workload_params.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/json
    ydb/library/json_index
    ydb/library/workload/abstract
    ydb/library/workload/benchmark_base
    ydb/public/sdk/cpp/src/client/types/status
)

END()
