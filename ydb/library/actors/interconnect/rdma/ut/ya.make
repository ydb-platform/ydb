UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ibv_ut.cpp
)

PEERDIR(
    ydb/library/actors/interconnect/rdma/ibdrv
)

END()
