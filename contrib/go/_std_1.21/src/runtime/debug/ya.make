GO_LIBRARY()

SRCS(
    debug.s
    garbage.go
    mod.go
    stack.go
    stubs.go
)

GO_XTEST_SRCS(
    garbage_test.go
    heapdump_test.go
    mod_test.go
    stack_test.go
)

IF (OS_LINUX)
    GO_XTEST_SRCS(panic_test.go)
ENDIF()

IF (OS_DARWIN)
    GO_XTEST_SRCS(panic_test.go)
ENDIF()

END()

RECURSE(
)
