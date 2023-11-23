GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(googlecloud.go)

GO_TEST_SRCS(googlecloud_test.go)

IF (OS_LINUX)
    SRCS(manufacturer_linux.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(manufacturer.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(manufacturer_windows.go)
ENDIF()

END()

RECURSE(gotest)
