LIBRARY()

SRCS(
    application.cpp
    kikimr_setup.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    library/cpp/json
    library/cpp/logger
    library/cpp/threading/future
    ydb/core/base
    ydb/core/blob_depot
    ydb/core/fq/libs/compute/common
    ydb/core/protos
    ydb/core/testlib
    ydb/library/actors/core
    ydb/library/actors/testlib
    ydb/library/grpc/server/actors
    ydb/library/services
    ydb/library/yql/providers/s3/actors
    ydb/public/api/protos
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/common
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
    yql/essentials/public/issue
    yql/essentials/public/udf
    yt/yql/providers/yt/mkql_dq
    yt/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

SUPPRESSIONS(
    lsan.supp
)

END()
