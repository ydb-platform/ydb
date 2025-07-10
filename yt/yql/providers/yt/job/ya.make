LIBRARY()

SRCS(
    yql_job_base.cpp
    yql_job_calc.cpp
    yql_job_factory.cpp
    yql_job_infer_schema.cpp
    yql_job_registry.h
    yql_job_stats_writer.cpp
    yql_job_user.cpp
    yql_job_user_base.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/streams/brotli
    library/cpp/time_provider
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/library/user_job_statistics
    yql/essentials/minikql/comp_nodes
    yql/essentials/public/langver
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/utils/backtrace
    yql/essentials/parser/pg_catalog
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/comp_nodes
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/schema/mkql
    yql/essentials/providers/common/schema/parser
    yt/yql/providers/yt/codec
    yt/yql/providers/yt/common
    yt/yql/providers/yt/comp_nodes
    yt/yql/providers/yt/lib/infer_schema
    yt/yql/providers/yt/lib/lambda_builder
    yt/yql/providers/yt/lib/mkql_helpers
)

YQL_LAST_ABI_VERSION()

END()
