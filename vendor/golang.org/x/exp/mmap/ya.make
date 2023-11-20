GO_LIBRARY()

LICENSE(BSD-3-Clause)

GO_TEST_SRCS(mmap_test.go)

IF (OS_LINUX)
    SRCS(mmap_unix.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(mmap_unix.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(mmap_windows.go)
ENDIF()

END()

RECURSE(gotest)
