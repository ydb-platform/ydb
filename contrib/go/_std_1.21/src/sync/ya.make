GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		cond.go
		map.go
		mutex.go
		once.go
		oncefunc.go
		pool.go
		poolqueue.go
		runtime.go
		runtime2.go
		rwmutex.go
		waitgroup.go
    )
ENDIF()
END()
