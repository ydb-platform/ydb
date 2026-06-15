LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    env.cpp
    exit.cpp
    process_id.cpp
    thread_id.cpp
    thread_name.cpp
)

PEERDIR(
    library/cpp/yt/exception
    library/cpp/yt/misc
)

END()

RECURSE(
    benchmarks
)

RECURSE_FOR_TESTS(
    unittests
)
