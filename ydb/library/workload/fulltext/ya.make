LIBRARY()

SRCS(
    fulltext.cpp
    fulltext_data_generator.cpp
    fulltext_workload_generator.cpp
    fulltext_workload_params.cpp
)

PEERDIR(
    ydb/library/workload/abstract
    ydb/library/workload/benchmark_base
)

END()
