GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/internal/common
)

SRCS(
    connection_string_ut.cpp
)

END()
