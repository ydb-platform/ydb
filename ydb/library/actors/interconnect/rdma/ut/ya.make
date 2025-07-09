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
    contrib/libs/ibdrv
)

END()
