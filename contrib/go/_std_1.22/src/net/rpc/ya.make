SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        client.go
        debug.go
        server.go
    )
ENDIF()
END()
