LIBRARY()

SRCS(
    tvm_client_dummy.cpp
)

PEERDIR(
    yt/yql/providers/yt/lib/tvm_client/dummy
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
