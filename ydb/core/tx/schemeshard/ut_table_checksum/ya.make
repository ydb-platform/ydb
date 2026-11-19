UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

# Avoid DWARF32 relocation overflow in this test executable
IF (OS_LINUX)
    LDFLAGS(-Wl,--strip-debug)
ENDIF()

SPLIT_FACTOR(10)
REQUIREMENTS(cpu:1)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

IF (NOT OS_WINDOWS)
    SRCS(
        export_ut.cpp
        import_ut.cpp
        snapshot_recovery_ut.cpp
        init.cpp
    )

    PEERDIR(
        library/cpp/json
        library/cpp/testing/hook
        ydb/core/backup/common
        ydb/core/backup/query
        ydb/core/kqp/common
        ydb/core/testlib/default
        ydb/core/tx/schemeshard/ut_helpers
        ydb/core/wrappers/ut_helpers
        ydb/library/aws_init
        ydb/library/yql/udfs/digest
        ydb/public/lib/value
        yql/essentials/ast
        yql/essentials/udfs/common/digest
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
