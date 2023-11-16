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

GO_XTEST_SRCS(
    atomic_test.go
    example_test.go
    value_test.go
)

END()

RECURSE(
)
