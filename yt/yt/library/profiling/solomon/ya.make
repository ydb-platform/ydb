LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    cube.cpp
    exporter.cpp
    helpers.cpp
    percpu.cpp
    producer.cpp
    proxy.cpp
    registry.cpp
    remote.cpp
    sensor.cpp
    sensor_service.cpp
    sensor_set.cpp
    tag_registry.cpp

    sensor_dump.proto
)

PEERDIR(
    yt/yt/library/profiling
    yt/yt/core
    yt/yt/core/http

    library/cpp/cgiparam
    library/cpp/monlib/metrics
    library/cpp/monlib/encode/prometheus
    library/cpp/monlib/encode/spack
    library/cpp/monlib/encode/json
    library/cpp/yt/threading
)

END()

RECURSE(
    proxy_example
)
