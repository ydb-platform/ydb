GO_LIBRARY()
IF (OS_WINDOWS AND ARCH_X86_64 AND RACE OR OS_WINDOWS AND ARCH_X86_64 AND NOT RACE)
    SRCS(
		memory_windows.go
		net_windows.go
		psapi_windows.go
		reparse_windows.go
		security_windows.go
		symlink_windows.go
		syscall_windows.go
		zsyscall_windows.go
    )
ENDIF()
END()
