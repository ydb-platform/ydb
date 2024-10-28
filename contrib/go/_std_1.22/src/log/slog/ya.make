SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        attr.go
        doc.go
        handler.go
        json_handler.go
        level.go
        logger.go
        record.go
        text_handler.go
        value.go
    )
ENDIF()
END()
