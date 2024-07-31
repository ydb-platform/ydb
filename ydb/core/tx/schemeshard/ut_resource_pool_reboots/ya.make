IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(ydb/core/tx/schemeshard)

    FORK_SUBTESTS()

    SPLIT_FACTOR(60)

    IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
        TIMEOUT(3600)
        SIZE(LARGE)
        TAG(ya:fat)
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

    REQUIREMENTS(ram:12)

END()
ENDIF()
