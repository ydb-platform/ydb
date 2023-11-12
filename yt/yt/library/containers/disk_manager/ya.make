LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
)

SRCS(
    config.cpp
    disk_info_provider.cpp
    disk_manager_proxy.cpp
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
