UNITTEST()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ENDIF()

SRCS(
    analyzer_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/services/sql/analyze
    ydb/services/sql/parse
)

YQL_LAST_ABI_VERSION()

END()
