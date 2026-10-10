PY3_PROGRAM(recipe)

STYLE_PYTHON()

PY_SRCS(
    __main__.py  
)

REQUIREMENTS(cpu:1)
PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common

    contrib/python/aiohttp
)

END()
