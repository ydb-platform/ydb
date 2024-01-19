GO_LIBRARY()

SRCS(
    doc.go
    type.go
    value.go
)

IF (RACE)
    SRCS(
        race.s
    )
ELSE()
    SRCS(
        asm.s
    )
ENDIF()

END()
