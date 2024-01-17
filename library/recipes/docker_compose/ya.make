PY3_PROGRAM()

PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/docker_compose/lib
)

PY_SRCS(
    __main__.py
)

END()


IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT == "ya" OR AUTOCHECK)
    # Don't export tests and examples to customers
    RECURSE_FOR_TESTS(
        example
        example_network_go
        example_test_container
        example_test_container_go
        example_with_context
        test
    )
ENDIF()
