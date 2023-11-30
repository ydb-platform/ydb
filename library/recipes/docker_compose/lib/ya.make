PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/PyYAML
    devtools/ya/test/const
    library/python/fs
    library/python/testing/recipe
)

END()
