UNITTEST_FOR(ydb/services/dynamic_config)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    dynamic_config_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
