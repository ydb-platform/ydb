GO_LIBRARY()

SRCS(
    doc.go
)

IF (RACE)
    IF (CGO_ENABLED OR OS_DARWIN)
        SRCS(
            race.go
        )
    ELSE()
        SRCS(
            norace.go
        )
    ENDIF()
ELSE()
    SRCS(
        norace.go
    )
ENDIF()


END()
