SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        description.go
        doc.go
        histogram.go
        sample.go
        value.go
    )
ENDIF()
END()
