PROGRAM(actors_core_ut_fat)

SRCDIR(
    ydb/library/actors/core
    ydb/library/actors/core/ut_fat
)

ADDINCL(
    ydb/library/actors/core
)

PEERDIR(
    library/cpp/testing/unittest_main
    ydb/library/actors/core
)

INCLUDE(${ARCADIA_ROOT}/ydb/library/actors/core/ut_fat/sources.inc)

END()
