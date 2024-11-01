SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        apis.go
        dummy.s
        emit.go
        hooks.go
        testsupport.go
    )
ENDIF()
END()
