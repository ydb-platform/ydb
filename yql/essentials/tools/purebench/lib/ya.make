YQL_LIBRARY()

SRCS(
    benchmark_program.cpp
    benchmark.cpp
)

PEERDIR(
    library/cpp/time_provider
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/mkql
    yql/essentials/public/langver
    yql/essentials/public/purecalc
    yql/essentials/public/purecalc/helpers/stream
    yql/essentials/public/purecalc/io_specs/arrow
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
)

END()

RECURSE_FOR_TESTS(
    ut
)
