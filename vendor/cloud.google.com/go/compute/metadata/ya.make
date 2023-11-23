GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    metadata.go
    retry.go
)

IF (OS_LINUX)
    SRCS(retry_linux.go)
ENDIF()

END()

RECURSE(internal)
