UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

# The linked server exceeds DWARF32 relocation limits in relwithdebinfo builds.
IF (OS_LINUX)
    LDFLAGS(-Wl,--strip-debug)
ENDIF()

SRCS(
    kqp_check_index_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/tx/datashard/ut_common
    ydb/library/json_index
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
