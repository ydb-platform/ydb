UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/tests/unit/client/draft/helpers
)

SRCS(
    ydb_scripting_response_headers_ut.cpp
    ydb_view_ut.cpp
)

END()
