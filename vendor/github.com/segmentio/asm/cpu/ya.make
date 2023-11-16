GO_LIBRARY()

LICENSE(MIT)

SRCS(cpu.go)

GO_XTEST_SRCS(cpu_test.go)

END()

RECURSE(
    arm
    arm64
    cpuid
    gotest
    x86
)
