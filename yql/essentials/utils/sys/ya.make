LIBRARY()

SRCS(
    become_user.h
    become_user_dummy.cpp
    linux_version.cpp
    linux_version.h
)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/libcap
    )

    SRCS(
        become_user.cpp
    )
ENDIF()

END()
