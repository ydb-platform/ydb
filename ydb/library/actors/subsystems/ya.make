LIBRARY()

SRCS(
    cgroup/cgroup_oom.cpp
    cgroup/cgroup_oom_trend.cpp
    cgroup/cgroup_v1.cpp
    cgroup/cgroup_v2.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/protos
    ydb/library/actors/util
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        ../core/tsan.supp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
