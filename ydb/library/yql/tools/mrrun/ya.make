PROGRAM()

ALLOCATOR(J)

SRCS(
    mrrun.h
    mrrun.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    contrib/libs/protobuf
    ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
    library/cpp/digest/md5
    library/cpp/getopt
    library/cpp/logger
    library/cpp/resource
    library/cpp/yson
    library/cpp/yson/node
    library/cpp/sighandler
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    ydb/core/util
    yql/essentials/sql/pg
    yql/essentials/core
    yql/essentials/core/facade
    yql/essentials/core/file_storage/proto
    yql/essentials/core/file_storage/http_download
    yql/essentials/core/file_storage
    yql/essentials/core/services/mounts
    yql/essentials/core/url_lister
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/opt
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/minikql/invoke_builtins/llvm14
    yql/essentials/protos
    ydb/library/yql/providers/clickhouse/actors
    ydb/library/yql/providers/clickhouse/provider
    yql/essentials/providers/common/comp_nodes
    ydb/library/yql/providers/common/http_gateway
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/udf_resolve
    ydb/library/yql/providers/dq/local_gateway
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/provider/exec
    ydb/library/yql/providers/dq/helper
    ydb/library/yql/providers/pq/async_io
    ydb/library/yql/providers/pq/gateway/native
    ydb/library/yql/providers/s3/actors
    ydb/library/yql/providers/ydb/actors
    ydb/library/yql/providers/ydb/comp_nodes
    ydb/library/yql/providers/ydb/provider
    yql/essentials/providers/pg/provider
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/utils/log
    yql/essentials/utils/backtrace
    yql/essentials/utils/failure_injector
    ydb/public/sdk/cpp/client/ydb_driver
    yql/essentials/core/url_preprocessing
    yt/yql/providers/yt/comp_nodes/dq
    yt/yql/providers/yt/comp_nodes/llvm14
    yt/yql/providers/yt/codec/codegen
    ydb/library/yql/providers/yt/dq_task_preprocessor
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/lib/log
    yt/yql/providers/yt/mkql_dq
    yt/yql/providers/yt/lib/yt_download
    yt/yql/providers/yt/lib/yt_url_lister
    yt/yql/providers/yt/lib/config_clusters
    yql/essentials/parser/pg_wrapper
)

IF (NOT OS_WINDOWS)
    PEERDIR(
        ydb/library/yql/providers/dq/global_worker_manager
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
