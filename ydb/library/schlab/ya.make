LIBRARY()

PEERDIR(
    ydb/library/schlab/schine
)

SRCS(
    defs.h
    schlab_actor.h
    schlab_actor.cpp
)

END()

RECURSE(
    mon
    probes
    schemu
    schine
    schoot
)

RECURSE_FOR_TESTS(
    ut
)
