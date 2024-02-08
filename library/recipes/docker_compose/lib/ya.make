PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    build/plugins/lib/test_const
    contrib/python/PyYAML
    library/python/fs
    library/python/testing/recipe
)

END()
