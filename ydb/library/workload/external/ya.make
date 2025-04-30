LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    data_generator.cpp
    external.cpp
)

PEERDIR(
    ydb/library/workload/benchmark_base
    library/cpp/json
)

END()
