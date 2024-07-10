LIBRARY()

SRCS(
    tpc_base.cpp
)

RESOURCE(
    ydb/library/benchmarks/gen_queries/consts.yql consts.yql
    ydb/library/benchmarks/gen_queries/consts_decimal.yql consts_decimal.yql
)

PEERDIR(
    ydb/library/accessor
    library/cpp/resource
    ydb/library/workload/benchmark_base
    ydb/public/lib/scheme_types
)

GENERATE_ENUM_SERIALIZATION(tpc_base.h)

END()
