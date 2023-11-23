LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    tvm_base.cpp
)

PEERDIR(
    library/cpp/yt/memory
)

IF(NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
