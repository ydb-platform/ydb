UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/table
)

SRCS(
    table_ut.cpp
)

END()
