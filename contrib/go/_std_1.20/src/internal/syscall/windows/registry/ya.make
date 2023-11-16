GO_LIBRARY()

BUILD_ONLY_IF(WARNING OS_WINDOWS)

IF (OS_WINDOWS)
    SRCS(
        key.go
        syscall.go
        value.go
        zsyscall_windows.go
    )
ENDIF()

END()
