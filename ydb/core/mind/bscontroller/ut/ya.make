UNITTEST_FOR(ydb/core/mind/bscontroller)

SRCS(
    grouper_ut.cpp
    group_mapper_ut.cpp
    mv_object_map_ut.cpp
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/actors/util
    ydb/core/yql_testlib
)

END()
