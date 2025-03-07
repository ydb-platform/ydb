IF (NOT OS_WINDOWS)
    PROGRAM()

IF (PROFILE_MEMORY_ALLOCATIONS)
    ALLOCATOR(LF_DBG)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ELSE()
    IF (OS_LINUX AND NOT DISABLE_TCMALLOC)
        ALLOCATOR(TCMALLOC_256K)
    ELSE()
        ALLOCATOR(J)
    ENDIF()
ENDIF()


IF (OOM_HELPER)
    PEERDIR(yql/essentials/utils/oom_helper)
ENDIF()

    SRCS(
        dqrun.cpp
    )

    PEERDIR(
        contrib/libs/protobuf
        ydb/public/sdk/cpp/src/client/persqueue_public/codecs
        ydb/library/actors/http
        library/cpp/getopt
        library/cpp/lfalloc/alloc_profiler
        library/cpp/logger
        library/cpp/resource
        library/cpp/yson
        library/cpp/digest/md5
        yt/cpp/mapreduce/interface
        yql/essentials/sql/pg
        yql/essentials/core/facade
        yql/essentials/core/file_storage
        yql/essentials/core/file_storage/proto
        yql/essentials/core/file_storage/http_download
        yql/essentials/core/services
        yql/essentials/core/services/mounts
        yql/essentials/sql
        yql/essentials/sql/v1
        yql/essentials/sql/v1/lexer/antlr4
        yql/essentials/sql/v1/lexer/antlr4_ansi
        yql/essentials/sql/v1/proto_parser/antlr4
        yql/essentials/sql/v1/proto_parser/antlr4_ansi
        ydb/library/yql/dq/actors/input_transforms
        ydb/library/yql/dq/comp_nodes
        ydb/library/yql/dq/opt
        yql/essentials/core/dq_integration/transform
        ydb/library/yql/dq/transform
        yql/essentials/minikql/comp_nodes/llvm16
        yql/essentials/minikql/invoke_builtins/llvm16
        ydb/library/yql/providers/clickhouse/actors
        ydb/library/yql/providers/clickhouse/provider
        yql/essentials/providers/common/comp_nodes
        yql/essentials/providers/common/proto
        ydb/library/yql/providers/common/token_accessor/client
        yql/essentials/providers/common/udf_resolve
        ydb/library/yql/providers/generic/actors
        ydb/library/yql/providers/generic/provider
        ydb/library/yql/providers/dq/local_gateway
        ydb/library/yql/providers/dq/provider
        ydb/library/yql/providers/dq/provider/exec
        ydb/library/yql/providers/dq/helper
        ydb/library/yql/providers/pq/async_io
        ydb/library/yql/providers/pq/gateway/dummy
        ydb/library/yql/providers/pq/gateway/native
        ydb/library/yql/providers/pq/provider
        ydb/library/yql/providers/s3/actors
        ydb/library/yql/providers/s3/provider
        ydb/library/yql/providers/solomon/actors
        ydb/library/yql/providers/solomon/gateway
        ydb/library/yql/providers/solomon/provider
        ydb/library/yql/providers/ydb/actors
        ydb/library/yql/providers/ydb/comp_nodes
        ydb/library/yql/providers/ydb/provider
        yql/essentials/providers/pg/provider

        yql/essentials/public/udf/service/exception_policy
        yql/essentials/utils/backtrace
        ydb/library/yql/utils/bindings
        yql/essentials/utils/log
        yql/essentials/utils/failure_injector
        yql/essentials/core/url_preprocessing
        yql/essentials/core/url_lister
        yql/essentials/core/pg_ext
        ydb/library/yql/providers/yt/actors
        yt/yql/providers/yt/comp_nodes/dq/llvm16
        ydb/library/yql/providers/yt/dq_task_preprocessor
        yt/yql/providers/yt/gateway/file
        yt/yql/providers/yt/gateway/native
        yt/yql/providers/yt/codec/codegen
        yt/yql/providers/yt/mkql_dq
        yt/yql/providers/yt/provider
        yt/yql/providers/yt/codec/codegen
        yt/yql/providers/yt/comp_nodes/llvm16
        yt/yql/providers/yt/lib/yt_download
        yt/yql/providers/yt/lib/yt_url_lister
        yt/yql/providers/yt/lib/config_clusters
        yql/essentials/parser/pg_wrapper
        yql/essentials/utils/log/proto
        yql/essentials/core/qplayer/storage/file
        yql/essentials/public/result_format

        ydb/library/yql/utils/actor_system
        ydb/core/fq/libs/actors
        ydb/core/fq/libs/db_id_async_resolver_impl
        ydb/core/fq/libs/init

        ydb/library/yql/udfs/common/clickhouse/client
    )

    YQL_LAST_ABI_VERSION()

    END()
ELSE()
    LIBRARY()

    END()
ENDIF()
