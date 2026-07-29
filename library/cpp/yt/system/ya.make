LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    cpu_id.cpp
    env.cpp
    exit.cpp
    process_id.cpp
    thread_id.cpp
    thread_name.cpp
)

PEERDIR(
    library/cpp/yt/cpu_clock
    library/cpp/yt/exception
    library/cpp/yt/misc
)

IF (OS_LINUX)
    PEERDIR(
        library/cpp/yt/rseq
    )
ENDIF()

END()

RECURSE(
    benchmarks
)

RECURSE_FOR_TESTS(
    unittests
)
