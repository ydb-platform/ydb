PY23_LIBRARY()

STYLE_PYTHON()

PEERDIR(
    library/cpp/svnversion
    contrib/python/future
)

PY_SRCS(
    __init__.py
    __svn_version.pyx
)

END()

RECURSE_FOR_TESTS(
    ut
)
