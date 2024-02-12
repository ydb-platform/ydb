GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		search.go
		slice.go
		sort.go
		zsortfunc.go
		zsortinterface.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		search.go
		slice.go
		sort.go
		zsortfunc.go
		zsortinterface.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		search.go
		slice.go
		sort.go
		zsortfunc.go
		zsortinterface.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		search.go
		slice.go
		sort.go
		zsortfunc.go
		zsortinterface.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		search.go
		slice.go
		sort.go
		zsortfunc.go
		zsortinterface.go
    )
ENDIF()
END()
