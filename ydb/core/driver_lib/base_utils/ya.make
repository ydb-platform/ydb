LIBRARY(base_utils)

SRCS(
    base_utils.h
    format_info.h
    format_info.cpp
    format_util.h
    format_util.cpp
    node_by_host.h
    node_by_host.cpp
)

PEERDIR(
    library/cpp/deprecated/enum_codegen
    ydb/library/grpc/client
    ydb/core/blobstorage/pdisk
    ydb/core/client/server
    ydb/core/driver_lib/cli_config_base
    ydb/core/protos
    ydb/public/lib/deprecated/client
)

YQL_LAST_ABI_VERSION()

END()
