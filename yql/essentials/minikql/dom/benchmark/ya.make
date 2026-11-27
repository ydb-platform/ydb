IF (NOT WINDOWS)
    G_BENCHMARK(dom-benchmark)

    SRCS(
        json_bench.cpp
        yson_bench.cpp
    )

    PEERDIR(
        yql/essentials/minikql/dom
        yql/essentials/minikql/dom/test_support
        yql/essentials/minikql/computation/llvm16
        yql/essentials/public/udf/service/exception_policy
        yql/essentials/sql/pg_dummy
    )

    SIZE(MEDIUM)

    YQL_LAST_ABI_VERSION()

    END()
ENDIF()
