UNITTEST_FOR(ydb/core/mind/bscontroller)

SRCS(
    blob_checker_planner_ut.cpp
    grouper_ut.cpp
    group_mapper_ut.cpp
    mv_object_map_ut.cpp
)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/library/actors/util
    ydb/core/yql_testlib
)

END()
