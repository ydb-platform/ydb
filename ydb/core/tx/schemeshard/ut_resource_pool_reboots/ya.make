IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(ydb/core/tx/schemeshard)

    FORK_SUBTESTS()

    SPLIT_FACTOR(60)

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        SIZE(LARGE)
        TAG(ya:fat)
        REQUIREMENTS(ram:12)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()

    PEERDIR(
        ydb/core/testlib/default
        ydb/core/tx/schemeshard/ut_helpers
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        ut_resource_pool_reboots.cpp
    )

END()
ENDIF()
