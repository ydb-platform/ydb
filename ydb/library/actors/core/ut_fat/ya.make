UNITTEST()

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TIMEOUT(2400)
    TAG(ya:fat)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(LARGE)
    TIMEOUT(1200)
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
