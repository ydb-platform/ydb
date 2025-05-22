UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_driver)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_table
)

SRCS(
    driver_ut.cpp
)

END()
