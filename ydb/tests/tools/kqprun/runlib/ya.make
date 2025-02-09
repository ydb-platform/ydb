LIBRARY()

SRCS(
    application.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    library/cpp/json
    util
    ydb/core/base
    ydb/core/blob_depot
    ydb/core/fq/libs/compute/common
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/actors/testlib
    ydb/library/services
    ydb/public/api/protos
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/common
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
