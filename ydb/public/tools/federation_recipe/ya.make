PY3_PROGRAM(federation_recipe)

SRCDIR(
    ydb/public/tools/federation_recipe
)

PY_SRCS(
    __main__.py
)

PEERDIR(
    library/python/testing/recipe
    ydb/tests/library/logbroker_federation
)

DEPENDS(
    ydb/public/tools/federation_recipe/bin
)

END()

RECURSE_FOR_TESTS(
    bin
)
