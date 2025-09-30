UNITTEST()

IF (OS_LINUX AND SANITIZER_TYPE != "memory")

SRCS(
    ibv_ut.cpp
)

PEERDIR(
    contrib/libs/ibdrv
)

ENDIF()

END()
