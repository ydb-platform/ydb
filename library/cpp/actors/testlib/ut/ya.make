UNITTEST_FOR(library/cpp/actors/testlib)

FORK_SUBTESTS()
SIZE(SMALL)


PEERDIR(
    library/cpp/actors/core
)

SRCS(
    decorator_ut.cpp
)

END()
