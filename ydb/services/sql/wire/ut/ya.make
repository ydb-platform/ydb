UNITTEST()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ENDIF()

SRCS(
    listener_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/services/sql/wire
)

YQL_LAST_ABI_VERSION()

END()
