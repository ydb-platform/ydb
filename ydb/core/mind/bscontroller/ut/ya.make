UNITTEST_FOR(ydb/core/mind/bscontroller)

SRCS(
    grouper_ut.cpp
    group_mapper_ut.cpp
    mv_object_map_ut.cpp
)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/library/actors/util
    ydb/core/yql_testlib
)

END()
