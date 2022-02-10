LIBRARY(commands) 
 
OWNER(g:kikimr) 
 
SRCS( 
    stock_workload.cpp
    ydb_command.cpp 
    ydb_profile.cpp 
    ydb_root_common.cpp 
    ydb_service_discovery.cpp 
    ydb_service_export.cpp 
    ydb_service_import.cpp 
    ydb_service_operation.cpp 
    ydb_service_scheme.cpp 
    ydb_service_scripting.cpp 
    ydb_service_stream.cpp 
    ydb_service_table.cpp 
    ydb_tools.cpp 
    ydb_workload.cpp
    ydb_yql.cpp 
) 
 
PEERDIR( 
    library/cpp/histogram/hdr
    library/cpp/protobuf/json 
    library/cpp/regex/pcre 
    library/cpp/threading/local_executor
    ydb/library/backup
    ydb/library/workload
    ydb/public/lib/operation_id
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/dump
    ydb/public/lib/ydb_cli/import
    ydb/public/sdk/cpp/client/draft
    ydb/public/sdk/cpp/client/ydb_discovery
    ydb/public/sdk/cpp/client/ydb_export
    ydb/public/sdk/cpp/client/ydb_import
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/ydb_persqueue_public
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
) 
 
END() 
