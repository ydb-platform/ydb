FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

PEERDIR(
    library/cpp/containers/concurrent_hash
    library/cpp/yt/containers
)

SRCS(
    main.cpp
)

END()
