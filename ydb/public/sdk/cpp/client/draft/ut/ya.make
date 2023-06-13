UNITTEST_FOR(ydb/public/sdk/cpp/client/draft)

IF (SANITIZER_TYPE)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    ydb_scripting_response_headers_ut.cpp
)

END()
