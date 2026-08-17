LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    tvm_service_common.cpp
    tvm_service_dummy.cpp
)

PEERDIR(
    yt/yt/library/tvm

    yt/yt/core

    library/cpp/yt/memory
    library/cpp/yt/logging
)

IF(NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()

RECURSE_FOR_TESTS(
    unittests
)
