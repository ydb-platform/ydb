LIBRARY()

SRCS(
    tvm_client_dummy.cpp
)

PEERDIR(
    yt/yql/providers/yt/lib/tvm_client/dummy
    yt/yql/providers/yt/lib/tvm_client/proto
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
