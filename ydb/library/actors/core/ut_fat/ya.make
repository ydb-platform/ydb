UNITTEST()

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(ya:fat)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(LARGE)
    TAG(ya:fat)
    SPLIT_FACTOR(20)
ENDIF()


PEERDIR(
    ydb/library/actors/core
)

SRCS(
    actor_benchmark.cpp
)

END()
