LIBRARY()

SRCS(
    vector_command_index.cpp
    vector_recall_evaluator.cpp
    vector_sampler.cpp
    vector_sql.cpp
    vector_workload_generator.cpp
    vector_workload_params.cpp
    vector.cpp
)

PEERDIR(
    ydb/library/workload/abstract
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(vector_enums.h)

END()
