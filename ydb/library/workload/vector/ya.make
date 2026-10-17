LIBRARY()

SRCS(
    configure_opts.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    vector_command_index.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    vector_data_generator.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    vector_recall_evaluator.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    vector_sampler.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    vector_sql.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    vector_workload_generator.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    vector_workload_params.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    vector.cpp  # gn: into ydb/public/lib/ydb_cli/commands
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/colorizer
    ydb/library/formats/arrow/csv/converter
    ydb/library/vector_distance
    ydb/library/workload/abstract
    ydb/library/workload/benchmark_base
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/types/status
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(vector_enums.h)

END()
