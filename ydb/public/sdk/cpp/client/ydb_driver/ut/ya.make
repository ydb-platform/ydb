UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_driver)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_table
)

SRCS(
    driver_ut.cpp
)

END()
