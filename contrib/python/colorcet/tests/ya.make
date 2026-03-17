PY3TEST()

PEERDIR(
    contrib/python/colorcet
)

NO_LINT()

SRCDIR(contrib/python/colorcet/colorcet/tests)

TEST_SRCS(
    __init__.py
    test_aliases.py
    test_bokeh.py
#    test_matplotlib.py
)

END()