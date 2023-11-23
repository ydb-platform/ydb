GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(cpuinfo.go)

IF (ARCH_X86_64)
    SRCS(
        cpuinfo_amd64.go
        cpuinfo_amd64.s
    )
ENDIF()

END()
