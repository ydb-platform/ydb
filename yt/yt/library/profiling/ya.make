LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    histogram_snapshot.cpp
    impl.cpp
    percpu.cpp
    producer.cpp
    sensor.cpp
    tag.cpp
    testing.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/cpu_clock
    library/cpp/yt/compact_containers
    library/cpp/yt/string
    library/cpp/yt/memory
    library/cpp/yt/threading
)

END()

RECURSE(
    perf
    sensors_owner
    solomon
    unittests
    example
    integration
    tcmalloc
)

IF (NOT OPENSOURCE)
    RECURSE(
        benchmark
    )
ENDIF()
