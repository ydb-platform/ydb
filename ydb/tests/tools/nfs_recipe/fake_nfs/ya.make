DLL(fake_nfs)

SRCS(
    fake_nfs.cpp
)

IF (OS_LINUX)
    LDFLAGS(-ldl)
ENDIF()

END()
