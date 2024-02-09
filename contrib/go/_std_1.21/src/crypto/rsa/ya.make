GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		notboring.go
		pkcs1v15.go
		pss.go
		rsa.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		notboring.go
		pkcs1v15.go
		pss.go
		rsa.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		notboring.go
		pkcs1v15.go
		pss.go
		rsa.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		notboring.go
		pkcs1v15.go
		pss.go
		rsa.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		notboring.go
		pkcs1v15.go
		pss.go
		rsa.go
    )
ENDIF()
END()
