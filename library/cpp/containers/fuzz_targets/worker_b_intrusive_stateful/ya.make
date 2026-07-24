FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

PEERDIR(
    library/cpp/cache
    library/cpp/yt/containers
)

SRCS(
    intrusive_stateful_fuzzing.cpp
)

END()
