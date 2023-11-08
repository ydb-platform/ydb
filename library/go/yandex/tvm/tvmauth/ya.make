GO_LIBRARY()

IF (CGO_ENABLED)
    USE_CXX()
    
    PEERDIR(
        library/cpp/tvmauth/client
        library/cpp/tvmauth/client/misc/api/dynamic_dst
    )


    SRCS(
        CGO_EXPORT
        tvm.cpp
    )

    CGO_SRCS(
        client.go
        logger.go
    )
ELSE()
    SRCS(
        stub.go
    )
ENDIF()

SRCS(
    doc.go
    types.go
)

GO_XTEST_SRCS(client_example_test.go)

END()

IF (CGO_ENABLED)
    RECURSE_FOR_TESTS(
        apitest
        gotest
        tiroletest
        tooltest
    )
ENDIF()
