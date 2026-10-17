LIBRARY()

SRCS(
    fulltext.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    fulltext_data_generator.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    fulltext_quality_evaluator.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    fulltext_workload_generator.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    fulltext_workload_params.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    markov_model_builder.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    markov_model_evaluator.cpp  # gn: into ydb/public/lib/ydb_cli/commands
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
