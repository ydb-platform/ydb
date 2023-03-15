UNITTEST_FOR(library/cpp/actors/helpers)

FORK_SUBTESTS()
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TIMEOUT(1200)
    TAG(ya:fat)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
    TIMEOUT(600)
    REQUIREMENTS(
        ram:16
    )
ENDIF()


PEERDIR(
    library/cpp/actors/interconnect
    library/cpp/actors/testlib
    library/cpp/actors/core
)

SRCS(
    selfping_actor_ut.cpp
)

END()
