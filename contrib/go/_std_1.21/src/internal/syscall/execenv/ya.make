GO_LIBRARY()

IF (OS_LINUX)
    SRCS(
        execenv_default.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        execenv_default.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        execenv_windows.go
    )
ENDIF()

END()
