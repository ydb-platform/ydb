GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
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
ELSEIF (OS_DARWIN AND ARCH_X86_64)
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
ELSEIF (OS_LINUX AND ARCH_AARCH64)
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
ELSEIF (OS_LINUX AND ARCH_X86_64)
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
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
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


RECURSE(
	atomic
)
