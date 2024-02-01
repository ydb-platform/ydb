GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		format.go
		format_rfc3339.go
		sleep.go
		sys_unix.go
		tick.go
		time.go
		zoneinfo.go
		zoneinfo_goroot.go
		zoneinfo_read.go
		zoneinfo_unix.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		format.go
		format_rfc3339.go
		sleep.go
		sys_unix.go
		tick.go
		time.go
		zoneinfo.go
		zoneinfo_goroot.go
		zoneinfo_read.go
		zoneinfo_unix.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		format.go
		format_rfc3339.go
		sleep.go
		sys_unix.go
		tick.go
		time.go
		zoneinfo.go
		zoneinfo_goroot.go
		zoneinfo_read.go
		zoneinfo_unix.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		format.go
		format_rfc3339.go
		sleep.go
		sys_unix.go
		tick.go
		time.go
		zoneinfo.go
		zoneinfo_goroot.go
		zoneinfo_read.go
		zoneinfo_unix.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		format.go
		format_rfc3339.go
		sleep.go
		sys_windows.go
		tick.go
		time.go
		zoneinfo.go
		zoneinfo_abbrs_windows.go
		zoneinfo_goroot.go
		zoneinfo_read.go
		zoneinfo_windows.go
    )
ENDIF()
END()


RECURSE(
	tzdata
)
