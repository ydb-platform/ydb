G_BENCHMARK()

YQL_CURRENT_ABI_VERSION()

SRCS(
    benchmark.cpp
)

PEERDIR(
    yql/essentials/tools/purebench/gbenchmark
)

DEPENDS(
    yql/essentials/udfs/common/digest/impl
)

DATA(
    arcadia/yql/essentials/tests/sql_benchmarks/allocator/suites
)

SIZE(MEDIUM)

END()
