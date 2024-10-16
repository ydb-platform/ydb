UNITTEST()

WITHOUT_LICENSE_TEXTS()

SUBSCRIBER(
    g:contrib
    g:cpp-contrib
)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/ibdrv
    )
ENDIF()

SRCS(
    init_ut.cpp
)

END()
