FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

PEERDIR(
    library/cpp/containers/compact_vector
    library/cpp/containers/stack_vector
    library/cpp/yt/compact_containers
)

SRCS(
    main.cpp
)

END()
