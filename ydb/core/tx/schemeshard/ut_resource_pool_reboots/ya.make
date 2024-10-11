IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(ydb/core/tx/schemeshard)

    FORK_SUBTESTS()

    SPLIT_FACTOR(60)

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        TIMEOUT(3600)
        SIZE(LARGE)
        TAG(ya:fat)
        REQUIREMENTS(ram:12)
    ELSE()
        TIMEOUT(600)
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
