LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    env.cpp
    exit.cpp
    thread_id.cpp
)

PEERDIR(
    library/cpp/yt/misc
)

END()

RECURSE_FOR_TESTS(
    unittests
)
