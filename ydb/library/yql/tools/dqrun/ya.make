IF (NOT OS_WINDOWS)
    PROGRAM()

IF (PROFILE_MEMORY_ALLOCATIONS)
    ALLOCATOR(LF_DBG)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ELSE()
    IF (OS_LINUX)
        ALLOCATOR(TCMALLOC_256K)
    ELSE()
        ALLOCATOR(J)
    ENDIF()
ENDIF()

    SRCS(
        dqrun.cpp
    )

    PEERDIR(
        contrib/libs/protobuf
        ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
        ydb/library/actors/http
        library/cpp/getopt
        library/cpp/lfalloc/alloc_profiler
        library/cpp/logger
        library/cpp/resource
        library/cpp/yson
        library/cpp/digest/md5
        yt/cpp/mapreduce/interface
        ydb/library/yql/sql/pg
        ydb/library/yql/core/facade
        ydb/library/yql/core/file_storage
        ydb/library/yql/core/file_storage/proto
        ydb/library/yql/core/file_storage/http_download
        ydb/library/yql/core/services
        ydb/library/yql/core/services/mounts
        ydb/library/yql/dq/actors/input_transforms
        ydb/library/yql/dq/comp_nodes
        ydb/library/yql/dq/actors/input_transforms
        ydb/library/yql/dq/integration/transform
        ydb/library/yql/dq/transform
        ydb/library/yql/minikql/comp_nodes/llvm14
        ydb/library/yql/minikql/invoke_builtins/llvm14
        ydb/library/yql/providers/clickhouse/actors
        ydb/library/yql/providers/clickhouse/provider
        ydb/library/yql/providers/common/comp_nodes
        ydb/library/yql/providers/common/proto
        ydb/library/yql/providers/common/token_accessor/client
        ydb/library/yql/providers/common/udf_resolve
        ydb/library/yql/providers/generic/actors
        ydb/library/yql/providers/generic/provider
        ydb/library/yql/providers/dq/local_gateway
        ydb/library/yql/providers/dq/provider
        ydb/library/yql/providers/dq/provider/exec
        ydb/library/yql/providers/pq/async_io
        ydb/library/yql/providers/pq/gateway/native
        ydb/library/yql/providers/pq/provider
        ydb/library/yql/providers/s3/actors
        ydb/library/yql/providers/s3/provider
        ydb/library/yql/providers/solomon/async_io
        ydb/library/yql/providers/solomon/gateway
        ydb/library/yql/providers/solomon/provider
        ydb/library/yql/providers/ydb/actors
        ydb/library/yql/providers/ydb/comp_nodes
        ydb/library/yql/providers/ydb/provider
        ydb/library/yql/providers/pg/provider

        ydb/library/yql/public/udf/service/terminate_policy
        ydb/library/yql/utils/backtrace
        ydb/library/yql/utils/bindings
        ydb/library/yql/utils/log
        ydb/library/yql/utils/failure_injector
        ydb/library/yql/core/url_preprocessing
        ydb/library/yql/core/url_lister
        ydb/library/yql/core/pg_ext
        ydb/library/yql/providers/yt/actors
        ydb/library/yql/providers/yt/comp_nodes/dq
        ydb/library/yql/providers/yt/dq_task_preprocessor
        ydb/library/yql/providers/yt/gateway/file
        ydb/library/yql/providers/yt/gateway/native
        ydb/library/yql/providers/yt/codec/codegen
        ydb/library/yql/providers/yt/mkql_dq
        ydb/library/yql/providers/yt/provider
        ydb/library/yql/providers/yt/codec/codegen
        ydb/library/yql/providers/yt/comp_nodes/llvm14
        ydb/library/yql/providers/yt/lib/yt_download
        ydb/library/yql/providers/yt/lib/yt_url_lister
        ydb/library/yql/providers/yt/lib/config_clusters
        ydb/library/yql/parser/pg_wrapper
        ydb/library/yql/utils/log/proto
        ydb/library/yql/core/qplayer/storage/file

        ydb/library/yql/utils/actor_system
        ydb/core/fq/libs/actors
        ydb/core/fq/libs/db_id_async_resolver_impl

        ydb/library/yql/udfs/common/clickhouse/client
    )

    YQL_LAST_ABI_VERSION()

    END()
ELSE()
    LIBRARY()

    END()
ENDIF()
