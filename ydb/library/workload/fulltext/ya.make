LIBRARY()

SRCS(
    fulltext.cpp
    fulltext_data_generator.cpp
    markov_model_builder.cpp
    fulltext_workload_generator.cpp
    fulltext_workload_params.cpp
    markov_model_evaluator.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/colorizer
    library/cpp/streams/factory/open_by_signature
    ydb/library/workload/abstract
    ydb/library/workload/benchmark_base
    ydb/public/sdk/cpp/src/client/types/status
)

END()
