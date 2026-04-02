UNITTEST_FOR(ydb/library/ycloud/impl)

FORK_SUBTESTS()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    library/cpp/retry
    ydb/core/testlib/default
    ydb/library/testlib/service_mocks
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
    folder_service_ut.cpp
    service_account_service_ut.cpp
    user_account_service_ut.cpp
)

END()
