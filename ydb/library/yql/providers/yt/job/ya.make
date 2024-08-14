LIBRARY()

SRCS(
    yql_job_base.cpp
    yql_job_calc.cpp
    yql_job_factory.cpp
    yql_job_infer_schema.cpp
    yql_job_registry.h
    yql_job_stats_writer.cpp
    yql_job_user.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/streams/brotli
    library/cpp/time_provider
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/library/user_job_statistics
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/utils/backtrace
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/common/schema/mkql
    ydb/library/yql/providers/common/schema/parser
    ydb/library/yql/providers/yt/codec
    ydb/library/yql/providers/yt/common
    ydb/library/yql/providers/yt/comp_nodes
    ydb/library/yql/providers/yt/lib/infer_schema
    ydb/library/yql/providers/yt/lib/lambda_builder
    ydb/library/yql/providers/yt/lib/mkql_helpers
)

YQL_LAST_ABI_VERSION()

END()
