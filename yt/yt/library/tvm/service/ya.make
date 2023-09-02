LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    tvm_service_common.cpp
    tvm_service_dummy.cpp
)

PEERDIR(
    library/cpp/yt/memory
    library/cpp/yt/logging
    yt/yt/core
)

IF(NOT OPENSOURCE)
    SRCS(
        GLOBAL tvm_service_yandex.cpp
    )

    PEERDIR(
        library/cpp/tvmauth
        library/cpp/tvmauth/client
        library/cpp/tvmauth/client/misc/api/dynamic_dst
        yt/yt/library/tvm
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    unittests
)
