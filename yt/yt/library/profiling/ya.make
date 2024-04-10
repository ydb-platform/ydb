LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    sensor.cpp
    producer.cpp
    impl.cpp
    tag.cpp
    testing.cpp
    histogram_snapshot.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/cpu_clock
    library/cpp/yt/small_containers
    library/cpp/yt/string
    library/cpp/yt/memory
)

END()

RECURSE(
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
