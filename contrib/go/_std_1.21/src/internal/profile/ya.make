GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		encode.go
		filter.go
		legacy_profile.go
		merge.go
		profile.go
		proto.go
		prune.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		encode.go
		filter.go
		legacy_profile.go
		merge.go
		profile.go
		proto.go
		prune.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		encode.go
		filter.go
		legacy_profile.go
		merge.go
		profile.go
		proto.go
		prune.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		encode.go
		filter.go
		legacy_profile.go
		merge.go
		profile.go
		proto.go
		prune.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
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
