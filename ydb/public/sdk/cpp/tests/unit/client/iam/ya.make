GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    iam_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/json
    library/cpp/testing/common
    ydb/public/sdk/cpp/src/client/iam
)

END()
