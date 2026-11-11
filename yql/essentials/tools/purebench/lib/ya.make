YQL_LIBRARY()

SRCS(
    benchmark_program.cpp
    benchmark.cpp
)

GENERATE_ENUM_SERIALIZATION(benchmark_program.h)

PEERDIR(
    library/cpp/time_provider
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/core
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/mkql
    yql/essentials/public/langver
    yql/essentials/public/purecalc
    yql/essentials/public/purecalc/io_specs/arrow
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
)

END()

RECURSE_FOR_TESTS(
    ut
)
