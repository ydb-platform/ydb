UNITTEST_FOR(ydb/public/sdk/cpp/client/impl/ydb_endpoints)

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

SRCS(
    endpoints_ut.cpp
)

END()
