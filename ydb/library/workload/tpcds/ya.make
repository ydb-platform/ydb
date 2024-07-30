LIBRARY()

SRCS(
#    driver.c
#    driver.cpp
#    GLOBAL data_generator.cpp
    GLOBAL registrar.cpp
    tpcds.cpp
)

RESOURCE(
    tpcds_schema.sql tpcds_schema.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q1.sql tpcds/yql/q1.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q2.sql tpcds/yql/q2.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q3.sql tpcds/yql/q3.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q4.sql tpcds/yql/q4.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q5.sql tpcds/yql/q5.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q6.sql tpcds/yql/q6.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q7.sql tpcds/yql/q7.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q8.sql tpcds/yql/q8.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q9.sql tpcds/yql/q9.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q20.sql tpcds/yql/q10.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q21.sql tpcds/yql/q11.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q22.sql tpcds/yql/q12.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q23.sql tpcds/yql/q13.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q24.sql tpcds/yql/q14.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q25.sql tpcds/yql/q15.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q26.sql tpcds/yql/q16.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q27.sql tpcds/yql/q17.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q28.sql tpcds/yql/q18.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q29.sql tpcds/yql/q19.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q20.sql tpcds/yql/q20.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q21.sql tpcds/yql/q21.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q22.sql tpcds/yql/q22.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q23.sql tpcds/yql/q23.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q24.sql tpcds/yql/q24.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q25.sql tpcds/yql/q25.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q26.sql tpcds/yql/q26.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q27.sql tpcds/yql/q27.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q28.sql tpcds/yql/q28.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q29.sql tpcds/yql/q29.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q30.sql tpcds/yql/q30.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q31.sql tpcds/yql/q31.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q32.sql tpcds/yql/q32.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q33.sql tpcds/yql/q33.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q34.sql tpcds/yql/q34.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q35.sql tpcds/yql/q35.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q36.sql tpcds/yql/q36.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q37.sql tpcds/yql/q37.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q38.sql tpcds/yql/q38.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q39.sql tpcds/yql/q39.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q40.sql tpcds/yql/q40.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q41.sql tpcds/yql/q41.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q42.sql tpcds/yql/q42.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q43.sql tpcds/yql/q43.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q44.sql tpcds/yql/q44.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q45.sql tpcds/yql/q45.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q46.sql tpcds/yql/q46.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q47.sql tpcds/yql/q47.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q48.sql tpcds/yql/q48.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q49.sql tpcds/yql/q49.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q50.sql tpcds/yql/q50.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q51.sql tpcds/yql/q51.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q52.sql tpcds/yql/q52.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q53.sql tpcds/yql/q53.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q54.sql tpcds/yql/q54.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q55.sql tpcds/yql/q55.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q56.sql tpcds/yql/q56.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q57.sql tpcds/yql/q57.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q58.sql tpcds/yql/q58.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q59.sql tpcds/yql/q59.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q60.sql tpcds/yql/q60.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q61.sql tpcds/yql/q61.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q62.sql tpcds/yql/q62.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q63.sql tpcds/yql/q63.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q64.sql tpcds/yql/q64.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q65.sql tpcds/yql/q65.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q66.sql tpcds/yql/q66.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q67.sql tpcds/yql/q67.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q68.sql tpcds/yql/q68.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q69.sql tpcds/yql/q69.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q70.sql tpcds/yql/q70.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q71.sql tpcds/yql/q71.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q72.sql tpcds/yql/q72.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q73.sql tpcds/yql/q73.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q74.sql tpcds/yql/q74.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q75.sql tpcds/yql/q75.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q76.sql tpcds/yql/q76.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q77.sql tpcds/yql/q77.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q78.sql tpcds/yql/q78.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q79.sql tpcds/yql/q79.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q80.sql tpcds/yql/q80.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q81.sql tpcds/yql/q81.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q82.sql tpcds/yql/q82.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q83.sql tpcds/yql/q83.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q84.sql tpcds/yql/q84.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q85.sql tpcds/yql/q85.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q86.sql tpcds/yql/q86.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q87.sql tpcds/yql/q87.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q88.sql tpcds/yql/q88.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q89.sql tpcds/yql/q89.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q90.sql tpcds/yql/q90.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q91.sql tpcds/yql/q91.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q92.sql tpcds/yql/q92.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q93.sql tpcds/yql/q93.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q94.sql tpcds/yql/q94.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q95.sql tpcds/yql/q95.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q96.sql tpcds/yql/q96.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q97.sql tpcds/yql/q97.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q98.sql tpcds/yql/q98.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpcds/yql/q99.sql tpcds/yql/q99.sql
)

PEERDIR(
    ydb/library/accessor
    library/cpp/resource
    ydb/library/workload/tpc_base
)

END()
