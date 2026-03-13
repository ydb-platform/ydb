UNITTEST_FOR(ydb/core/kqp)

<<<<<<< HEAD
=======
IF (SANITIZER_TYPE OR WITH_VALGRIND)
    REQUIREMENTS(cpu:2)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

>>>>>>> 7bf789f021c (Main: Optimisation for medium and small tests cpu requirments (without split and fork) (#35835))
FORK_SUBTESTS()

SRCS(
    kqp_generic_provider_ut.cpp
    iceberg_ut_data.cpp
    iceberg_ut_data.h
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/fmt
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    ydb/library/yql/providers/s3/actors
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
