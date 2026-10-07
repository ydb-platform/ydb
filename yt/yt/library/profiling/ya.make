LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    histogram_snapshot.cpp
    impl.cpp
    per_cpu_sensor_impl.cpp
    producer.cpp
    sensor.cpp
    simple_sensor_impl.cpp
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

# The rseq-backed sensors use library/cpp/yt/rseq, which is Linux-only.
IF (OS_LINUX)
    SRCS(
        rseq_sensor_impl.cpp
    )
    PEERDIR(
        library/cpp/yt/rseq
    )
ENDIF()

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
