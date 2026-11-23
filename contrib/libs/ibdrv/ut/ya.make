UNITTEST()

WITHOUT_LICENSE_TEXTS()

SUBSCRIBER(
    g:contrib
    g:cpp-contrib
)

ADDINCL(
    contrib/libs/ibdrv/include/ibdrv
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
