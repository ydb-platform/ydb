PY3_PROGRAM(recipe)

STYLE_PYTHON()

PY_SRCS(
    __main__.py  
)

PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common

    contrib/python/aiohttp
)

END()
