LIBRARY()

SRCS(
    clickbench.cpp
    data_generator.cpp
    GLOBAL registrar.cpp
)

RESOURCE(
    click_bench_queries.sql click_bench_queries.sql
    ${ARCADIA_ROOT}/ydb/tests/functional/clickbench/data/queries-deterministic.sql queries-deterministic.sql
    click_bench_schema.sql click_bench_schema.sql
    click_bench_canonical/q0.result click_bench_canonical/q0.result
    click_bench_canonical/q1.result click_bench_canonical/q1.result
    click_bench_canonical/q2.result click_bench_canonical/q2.result
    click_bench_canonical/q3.result click_bench_canonical/q3.result
    click_bench_canonical/q4.result click_bench_canonical/q4.result
    click_bench_canonical/q5.result click_bench_canonical/q5.result
    click_bench_canonical/q6.result click_bench_canonical/q6.result
    click_bench_canonical/q7.result click_bench_canonical/q7.result
    click_bench_canonical/q8.result click_bench_canonical/q8.result
    click_bench_canonical/q9.result click_bench_canonical/q9.result
    click_bench_canonical/q10.result click_bench_canonical/q10.result
    click_bench_canonical/q11.result click_bench_canonical/q11.result
    click_bench_canonical/q12.result click_bench_canonical/q12.result
    click_bench_canonical/q13.result click_bench_canonical/q13.result
    click_bench_canonical/q14.result click_bench_canonical/q14.result
    click_bench_canonical/q15.result click_bench_canonical/q15.result
    click_bench_canonical/q16.result click_bench_canonical/q16.result
    click_bench_canonical/q17.result click_bench_canonical/q17.result
    click_bench_canonical/q18.result click_bench_canonical/q18.result
    click_bench_canonical/q19.result click_bench_canonical/q19.result
    click_bench_canonical/q20.result click_bench_canonical/q20.result
    click_bench_canonical/q21.result click_bench_canonical/q21.result
    click_bench_canonical/q22.result click_bench_canonical/q22.result
    click_bench_canonical/q23.result click_bench_canonical/q23.result
    click_bench_canonical/q24.result click_bench_canonical/q24.result
    click_bench_canonical/q25.result click_bench_canonical/q25.result
    click_bench_canonical/q26.result click_bench_canonical/q26.result
    click_bench_canonical/q27.result click_bench_canonical/q27.result
    click_bench_canonical/q28.result click_bench_canonical/q28.result
    click_bench_canonical/q29.result click_bench_canonical/q29.result
    click_bench_canonical/q30.result click_bench_canonical/q30.result
    click_bench_canonical/q31.result click_bench_canonical/q31.result
    click_bench_canonical/q32.result click_bench_canonical/q32.result
    click_bench_canonical/q33.result click_bench_canonical/q33.result
    click_bench_canonical/q34.result click_bench_canonical/q34.result
    click_bench_canonical/q35.result click_bench_canonical/q35.result
    click_bench_canonical/q36.result click_bench_canonical/q36.result
    click_bench_canonical/q37.result click_bench_canonical/q37.result
    click_bench_canonical/q38.result click_bench_canonical/q38.result
    click_bench_canonical/q39.result click_bench_canonical/q39.result
    click_bench_canonical/q40.result click_bench_canonical/q40.result
    click_bench_canonical/q41.result click_bench_canonical/q41.result
    click_bench_canonical/q42.result click_bench_canonical/q42.result
)

PEERDIR(
    ydb/library/accessor
    ydb/library/workload/benchmark_base
    library/cpp/http/io
    library/cpp/resource
    library/cpp/streams/factory/open_by_signature
)

END()
