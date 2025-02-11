LIBRARY(common)

SRCS(
    aws.cpp
    command.cpp
    command_utils.cpp
    common.cpp
    csv_parser.cpp
    examples.cpp
    format.cpp
    interactive.cpp
    interruptible.cpp
    normalize_path.cpp
    parameter_stream.cpp
    parameters.cpp
    pg_dump_parser.cpp
    plan2svg.cpp
    pretty_table.cpp
    print_operation.cpp
    print_utils.cpp
    profile_manager.cpp
    progress_bar.cpp
    progress_indication.cpp
    query_stats.cpp
    recursive_list.cpp
    recursive_remove.cpp
    retry_func.cpp
    root.cpp
    scheme_printers.cpp
    sys.cpp
    tabbed_table.cpp
    waiting_bar.cpp
    yt.cpp
)

IF (YDB_CERTIFIED)
    CFLAGS(
        -DDISABLE_UPDATE
    )
ELSE()
    SRCS(
        ydb_updater.cpp
    )
ENDIF ()

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    library/cpp/config
    library/cpp/getopt
    library/cpp/json/writer
    library/cpp/yaml/as
    library/cpp/string_utils/csv
    ydb/public/lib/json_value
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/client/types/credentials
    ydb/public/sdk/cpp/src/client/types/credentials/oauth2_token_exchange
    ydb/library/arrow_parquet
)

GENERATE_ENUM_SERIALIZATION(formats.h)
GENERATE_ENUM_SERIALIZATION(parameters.h)

END()

RECURSE_FOR_TESTS(
    ut
)
