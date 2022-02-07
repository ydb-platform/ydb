PY3TEST()

OWNER(g:python-contrib)

PEERDIR(
    contrib/python/more-itertools
)

TEST_SRCS(
    test_more.py
    test_recipes.py
)

NO_LINT()

END()
