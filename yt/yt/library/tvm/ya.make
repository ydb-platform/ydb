LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    tvm_base.cpp
)

PEERDIR(
    library/cpp/yt/memory
)

IF(NOT OPENSOURCE)
    SRCS(
        tvm.cpp
    )

    PEERDIR(
        library/cpp/tvmauth/client
    )
ENDIF()

END()
