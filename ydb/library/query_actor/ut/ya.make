UNITTEST_FOR(ydb/library/query_actor)

# Discard .debug_aranges to avoid DWARF32 relocation overflow
IF (OS_LINUX)
    LINKER_SCRIPT(${ARCADIA_ROOT}/ydb/apps/ydbd/strip_debug.ld)
ENDIF()

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

PEERDIR(
    ydb/core/kqp/compute_actor
    ydb/core/kqp/node_service
    ydb/core/kqp/ut/common
    ydb/core/testlib
    ydb/library/yql/dq/actors/compute
    yql/essentials/sql/pg_dummy
)

SRCS(
    checksum_table_ut.cpp
    mvcc_version_ut.cpp
    query_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
