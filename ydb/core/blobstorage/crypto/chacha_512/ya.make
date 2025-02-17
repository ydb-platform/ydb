LIBRARY()


IF (NOT OS_WINDOWS AND NOT ARCH_ARM64)
    SRCS(
        chacha_512.cpp
    )
    CFLAGS(
        -mavx512f
    )
ENDIF()

END()
