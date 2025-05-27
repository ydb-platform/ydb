SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        cgi_main.go
        child.go
        host.go
    )
ENDIF()
END()
