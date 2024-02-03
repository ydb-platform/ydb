GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		encode.go
		filter.go
		legacy_profile.go
		merge.go
		profile.go
		proto.go
		prune.go
    )
ENDIF()
END()
