LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    cgroup.cpp
    config.cpp
    instance.cpp
    instance_limits_tracker.cpp
    process.cpp
    porto_executor.cpp
    porto_resource_tracker.cpp
    porto_health_checker.cpp
)

PEERDIR(
    library/cpp/porto/proto
    yt/yt/core
)

IF(OS_LINUX)
    PEERDIR(
        library/cpp/porto
    )
ENDIF()

END()

RECURSE(
    disk_manager
    cri
)

RECURSE_FOR_TESTS(
    unittests
)
