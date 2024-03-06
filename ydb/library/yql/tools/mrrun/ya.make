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
    ydb/library/yql/sql/pg
    ydb/library/yql/core
    ydb/library/yql/core/facade
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/core/file_storage/http_download
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/services/mounts
    ydb/library/yql/core/url_lister
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/integration/transform
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/protos
    ydb/library/yql/providers/clickhouse/actors
    ydb/library/yql/providers/clickhouse/provider
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/udf_resolve
    ydb/library/yql/providers/dq/local_gateway
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/provider/exec
    ydb/library/yql/providers/pq/async_io
    ydb/library/yql/providers/s3/actors
    ydb/library/yql/providers/ydb/actors
    ydb/library/yql/providers/ydb/comp_nodes
    ydb/library/yql/providers/ydb/provider
    ydb/library/yql/providers/pg/provider
    ydb/library/yql/public/udf/service/terminate_policy
    ydb/library/yql/utils/log
    ydb/library/yql/utils/backtrace
    ydb/library/yql/utils/failure_injector
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/library/yql/core/url_preprocessing
    ydb/library/yql/providers/yt/comp_nodes/dq
    ydb/library/yql/providers/yt/comp_nodes/llvm14
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/providers/yt/dq_task_preprocessor
    ydb/library/yql/providers/yt/gateway/native
    ydb/library/yql/providers/yt/lib/log
    ydb/library/yql/providers/yt/mkql_dq
    ydb/library/yql/providers/yt/lib/yt_download
    ydb/library/yql/providers/yt/lib/yt_url_lister
    ydb/library/yql/providers/yt/lib/config_clusters
    ydb/library/yql/parser/pg_wrapper
)

IF (NOT OS_WINDOWS)
    PEERDIR(
        ydb/library/yql/providers/dq/global_worker_manager
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
