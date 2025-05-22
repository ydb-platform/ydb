UNITTEST()

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
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/params
)

END()
