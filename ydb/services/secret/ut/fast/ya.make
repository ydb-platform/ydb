UNITTEST_FOR(ydb/services/secret)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    ydb/services/secret
    ydb/services/secret/ut/common
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

SRCS(
    describe_schema_secrets_service_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
