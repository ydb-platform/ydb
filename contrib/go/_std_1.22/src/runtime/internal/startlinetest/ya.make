SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (OS_DARWIN AND ARCH_X86_64 AND RACE AND CGO_ENABLED OR OS_DARWIN AND ARCH_X86_64 AND RACE AND NOT CGO_ENABLED OR OS_DARWIN AND ARCH_X86_64 AND NOT RACE AND CGO_ENABLED OR OS_DARWIN AND ARCH_X86_64 AND NOT RACE AND NOT CGO_ENABLED OR OS_LINUX AND ARCH_X86_64 AND RACE AND CGO_ENABLED OR OS_LINUX AND ARCH_X86_64 AND RACE AND NOT CGO_ENABLED OR OS_LINUX AND ARCH_X86_64 AND NOT RACE AND CGO_ENABLED OR OS_LINUX AND ARCH_X86_64 AND NOT RACE AND NOT CGO_ENABLED OR OS_WINDOWS AND ARCH_X86_64 AND RACE AND CGO_ENABLED OR OS_WINDOWS AND ARCH_X86_64 AND RACE AND NOT CGO_ENABLED OR OS_WINDOWS AND ARCH_X86_64 AND NOT RACE AND CGO_ENABLED OR OS_WINDOWS AND ARCH_X86_64 AND NOT RACE AND NOT CGO_ENABLED)
    SRCS(
        func_amd64.go
        func_amd64.s
    )
ENDIF()
END()
