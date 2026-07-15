LIBRARY()

SRCS(
    configure_opts.cpp
    vector_command_index.cpp
    vector_data_generator.cpp
    vector_recall_evaluator.cpp
    vector_sampler.cpp
    vector_sql.cpp
    vector_workload_generator.cpp
    vector_workload_params.cpp
    vector.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/colorizer
    ydb/library/formats/arrow/csv/converter
    ydb/library/workload/abstract
    ydb/library/workload/benchmark_base
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/types/status
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(vector_enums.h)

END()
