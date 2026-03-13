UNITTEST()

IF (OS_LINUX AND SANITIZER_TYPE != "memory")
    REQUIREMENTS(cpu:4)

SRCS(
    ibv_ut.cpp
)

PEERDIR(
    contrib/libs/ibdrv
)

ENDIF()

END()
