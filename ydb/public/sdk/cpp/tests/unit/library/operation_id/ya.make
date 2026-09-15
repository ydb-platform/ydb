GTEST()

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    operation_id_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/operation_id
)

END()
