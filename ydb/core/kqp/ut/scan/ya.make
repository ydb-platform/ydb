UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_flowcontrol_ut.cpp
    kqp_scan_ut.cpp
    kqp_split_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
