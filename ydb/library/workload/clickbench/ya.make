LIBRARY()

SRCS(
    clickbench.cpp
    data_generator.cpp
    GLOBAL registrar.cpp
)

RESOURCE(
    click_bench_queries.sql click_bench_queries.sql
    click_bench_schema.sql click_bench_schema.sql
    click_bench_canonical/q0.result q0.result
    click_bench_canonical/q1.result q1.result
    click_bench_canonical/q2.result q2.result
    click_bench_canonical/q3.result q3.result
    click_bench_canonical/q4.result q4.result
    click_bench_canonical/q5.result q5.result
    click_bench_canonical/q6.result q6.result
    click_bench_canonical/q7.result q7.result
    click_bench_canonical/q8.result q8.result
    click_bench_canonical/q9.result q9.result
    click_bench_canonical/q10.result q10.result
    click_bench_canonical/q11.result q11.result
    click_bench_canonical/q12.result q12.result
    click_bench_canonical/q13.result q13.result
    click_bench_canonical/q14.result q14.result
    click_bench_canonical/q15.result q15.result
    click_bench_canonical/q16.result q16.result
    click_bench_canonical/q17.result q17.result
    click_bench_canonical/q18.result q18.result
    click_bench_canonical/q19.result q19.result
    click_bench_canonical/q20.result q20.result
    click_bench_canonical/q21.result q21.result
    click_bench_canonical/q22.result q22.result
    click_bench_canonical/q23.result q23.result
    click_bench_canonical/q24.result q24.result
    click_bench_canonical/q25.result q25.result
    click_bench_canonical/q26.result q26.result
    click_bench_canonical/q27.result q27.result
    click_bench_canonical/q28.result q28.result
    click_bench_canonical/q29.result q29.result
    click_bench_canonical/q30.result q30.result
    click_bench_canonical/q31.result q31.result
    click_bench_canonical/q32.result q32.result
    click_bench_canonical/q33.result q33.result
    click_bench_canonical/q34.result q34.result
    click_bench_canonical/q35.result q35.result
    click_bench_canonical/q36.result q36.result
    click_bench_canonical/q37.result q37.result
    click_bench_canonical/q38.result q38.result
    click_bench_canonical/q39.result q39.result
    click_bench_canonical/q40.result q40.result
    click_bench_canonical/q41.result q41.result
    click_bench_canonical/q42.result q42.result
)

PEERDIR(
    ydb/library/accessor
    ydb/library/workload/benchmark_base
    library/cpp/http/io
    library/cpp/resource
    library/cpp/streams/factory/open_by_signature
)

END()
