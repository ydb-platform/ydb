UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_result)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    result_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_params
)

END()
