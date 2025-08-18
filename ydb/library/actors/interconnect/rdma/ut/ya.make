UNITTEST()

IF (OS_LINUX)

SRCS(
    ibv_ut.cpp
)

PEERDIR(
    contrib/libs/ibdrv
)

ENDIF()

END()
