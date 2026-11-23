G_BENCHMARK(runtime-settings-benchmark)

SRCS(
    runtime_settings_bench.cpp
)

PEERDIR(
    yql/essentials/minikql/runtime_settings
    yql/essentials/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()
