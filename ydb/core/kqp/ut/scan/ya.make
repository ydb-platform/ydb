UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (NOT SANITIZER_TYPE)
    REQUIREMENTS(cpu:1)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()
SIZE(MEDIUM)

SRCS(
    kqp_flowcontrol_ut.cpp
    kqp_scan_ut.cpp
    kqp_snapshot_ranges_ut.cpp
    kqp_split_ut.cpp
    kqp_point_consolidation_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
