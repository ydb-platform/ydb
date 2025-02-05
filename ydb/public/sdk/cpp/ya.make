RECURSE(
    adapters
    examples
    src
)

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT != "ydb")
    RECURSE(
        client
    )
ENDIF()

RECURSE_FOR_TESTS(
    tests
)
