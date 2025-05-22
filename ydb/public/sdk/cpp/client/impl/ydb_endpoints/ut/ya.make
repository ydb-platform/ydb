UNITTEST_FOR(ydb/public/sdk/cpp/client/impl/ydb_endpoints)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    endpoints_ut.cpp
)

END()
