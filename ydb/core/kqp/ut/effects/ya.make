UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
    IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
        REQUIREMENTS(cpu:1)
    ELSE()
        REQUIREMENTS(cpu:2)
    ENDIF()
ENDIF()

SRCS(
    kqp_connection_ut.cpp
    kqp_effects_ut.cpp
    kqp_immediate_effects_ut.cpp
    kqp_inplace_update_ut.cpp
    kqp_overload_ut.cpp
    kqp_reattach_ut.cpp
    kqp_write_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
