LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    vector_recall_evaluator.cpp
    vector_sql.cpp
    vector_workload_generator.cpp
    vector_workload_params.cpp
)

PEERDIR(
    ydb/library/workload/abstract
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(vector_workload_generator.h)

END()
