UNITTEST_FOR(ydb/library/ycloud/impl)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/retry
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
    folder_service_ut.cpp
    service_account_service_ut.cpp
    user_account_service_ut.cpp
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:10)
ENDIF()

END()
