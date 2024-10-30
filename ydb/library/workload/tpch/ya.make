LIBRARY()

SRCS(
    driver.c
    driver.cpp
    GLOBAL data_generator.cpp
    GLOBAL registrar.cpp
    tpch.cpp
)

IF (OS_MACOS OR OS_DARWIN)
    CFLAGS(-DLINUX)
ELSEIF (OS_WINDOWS)
    CONLYFLAGS(-DWIN32)
    CXXFLAGS(-D_POSIX_ -DLINUX)
ELSEIF (OS_LINUX)
    CONLYFLAGS(-D_POSIX_SOURCE)
    CFLAGS(-DLINUX)
ENDIF()

RESOURCE(
    tpch_schema.sql tpch_schema.sql
    ydb/library/benchmarks/gen/tpch-dbgen/dists.dss dists.dss
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q1.sql tpch/yql/q1.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q2.sql tpch/yql/q2.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q3.sql tpch/yql/q3.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q4.sql tpch/yql/q4.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q5.sql tpch/yql/q5.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q6.sql tpch/yql/q6.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q7.sql tpch/yql/q7.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q8.sql tpch/yql/q8.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q9.sql tpch/yql/q9.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q10.sql tpch/yql/q10.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q11.sql tpch/yql/q11.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q12.sql tpch/yql/q12.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q13.sql tpch/yql/q13.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q14.sql tpch/yql/q14.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q15.sql tpch/yql/q15.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q16.sql tpch/yql/q16.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q17.sql tpch/yql/q17.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q18.sql tpch/yql/q18.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q19.sql tpch/yql/q19.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q20.sql tpch/yql/q20.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q21.sql tpch/yql/q21.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/yql/q22.sql tpch/yql/q22.sql

    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q1.sql tpch/pg/q1.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q2.sql tpch/pg/q2.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q3.sql tpch/pg/q3.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q4.sql tpch/pg/q4.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q5.sql tpch/pg/q5.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q6.sql tpch/pg/q6.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q7.sql tpch/pg/q7.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q8.sql tpch/pg/q8.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q9.sql tpch/pg/q9.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q10.sql tpch/pg/q10.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q11.sql tpch/pg/q11.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q12.sql tpch/pg/q12.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q13.sql tpch/pg/q13.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q14.sql tpch/pg/q14.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q15.sql tpch/pg/q15.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q16.sql tpch/pg/q16.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q17.sql tpch/pg/q17.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q18.sql tpch/pg/q18.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q19.sql tpch/pg/q19.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q20.sql tpch/pg/q20.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q21.sql tpch/pg/q21.sql
    ${ARCADIA_ROOT}/ydb/library/benchmarks/queries/tpch/pg/q22.sql tpch/pg/q22.sql
)

ALL_RESOURCE_FILES_FROM_DIRS(
    PREFIX tpch/
    s1_canonical
    s10_canonical
    s100_canonical
    s1000_canonical
    s10000_canonical
)

PEERDIR(
    contrib/libs/fmt
    ydb/library/accessor
    library/cpp/resource
    ydb/library/benchmarks/gen/tpch-dbgen
    ydb/library/workload/tpc_base
)

END()

RECURSE_FOR_TESTS(ut)
