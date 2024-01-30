GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		allocs.go
		benchmark.go
		cover.go
		example.go
		fuzz.go
		match.go
		newcover.go
		run_example.go
		testing.go
		testing_other.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		allocs.go
		benchmark.go
		cover.go
		example.go
		fuzz.go
		match.go
		newcover.go
		run_example.go
		testing.go
		testing_other.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		allocs.go
		benchmark.go
		cover.go
		example.go
		fuzz.go
		match.go
		newcover.go
		run_example.go
		testing.go
		testing_other.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		allocs.go
		benchmark.go
		cover.go
		example.go
		fuzz.go
		match.go
		newcover.go
		run_example.go
		testing.go
		testing_other.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		allocs.go
		benchmark.go
		cover.go
		example.go
		fuzz.go
		match.go
		newcover.go
		run_example.go
		testing.go
		testing_other.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		allocs.go
		benchmark.go
		cover.go
		example.go
		fuzz.go
		match.go
		newcover.go
		run_example.go
		testing.go
		testing_other.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		allocs.go
		benchmark.go
		cover.go
		example.go
		fuzz.go
		match.go
		newcover.go
		run_example.go
		testing.go
		testing_windows.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		allocs.go
		benchmark.go
		cover.go
		example.go
		fuzz.go
		match.go
		newcover.go
		run_example.go
		testing.go
		testing_windows.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		allocs.go
		benchmark.go
		cover.go
		example.go
		fuzz.go
		match.go
		newcover.go
		run_example.go
		testing.go
		testing_windows.go
    )
ENDIF()
END()


RECURSE(
	fstest
	internal
	iotest
	quick
	slogtest
)
