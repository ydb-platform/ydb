GO_LIBRARY()

LICENSE(MIT)

SRCS(
    bufferqueue.go
    nbconn.go
)

GO_XTEST_SRCS(nbconn_test.go)

IF (OS_LINUX)
    SRCS(nbconn_real_non_block.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(nbconn_real_non_block.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(nbconn_fake_non_block.go)
ENDIF()

END()

RECURSE(gotest)
