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
    ydb/library/workload/abstract
    ydb/public/api/protos
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(vector_enums.h)

END()
