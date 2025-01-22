RECURSE(
    adapters
    examples
    src
)

IF (NOT OPENSOURCE)
    RECURSE(
        client
    )
ENDIF()

RECURSE_FOR_TESTS(
    tests
)
