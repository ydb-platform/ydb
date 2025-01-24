SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        chacha8.go
        exp.go
        normal.go
        pcg.go
        rand.go
        zipf.go
    )
ENDIF()
END()
