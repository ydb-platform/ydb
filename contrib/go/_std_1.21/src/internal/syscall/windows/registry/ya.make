GO_LIBRARY()
IF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		key.go
		syscall.go
		value.go
		zsyscall_windows.go
    )
ENDIF()
END()
