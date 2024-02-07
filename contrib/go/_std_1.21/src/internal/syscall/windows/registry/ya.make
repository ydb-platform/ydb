GO_LIBRARY()
IF (OS_WINDOWS AND ARCH_X86_64 AND RACE OR OS_WINDOWS AND ARCH_X86_64 AND NOT RACE)
    SRCS(
		key.go
		syscall.go
		value.go
		zsyscall_windows.go
    )
ENDIF()
END()
