GO_LIBRARY()

NO_COMPILER_WARNINGS()

SRCS(
    doc.go
)

IF (ARCH_X86_64)
    SRCS(
        race_v1_amd64.go
    )
ENDIF()

IF (OS_DARWIN)
    IF (ARCH_X86_64)
        SRCS(
            race_darwin_amd64.go
        )
    ENDIF()
    IF (ARCH_ARM64)
        SRCS(
            race_darwin_arm64.go
            race_darwin_arm64.syso
        )
    ENDIF()
ENDIF()

IF (OS_LINUX)
    IF (ARCH_ARM64)
        SRCS(
            race_linux_arm64.syso
        )
    ENDIF()
ENDIF()

IF (RACE)
    IF (CGO_ENABLED OR OS_DARWIN)
        CGO_SRCS(
            race.go
        )
    ENDIF()
ENDIF()

END()

IF (ARCH_X86_64)
    RECURSE(
        internal
    )
ENDIF()
