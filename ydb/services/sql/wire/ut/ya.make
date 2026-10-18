UNITTEST()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ENDIF()

SRCS(
    create_table_ut.cpp
    listener_ut.cpp
    schemata_ut.cpp
    select_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/services/sql/wire
)

YQL_LAST_ABI_VERSION()

END()
