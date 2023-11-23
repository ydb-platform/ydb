GO_LIBRARY()

LICENSE(Apache-2.0)

IF (OS_LINUX)
    SRCS(trap_others.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(trap_others.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(trap_windows.go)
ENDIF()

END()
