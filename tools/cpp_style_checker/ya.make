PY3_PROGRAM()

PEERDIR(
    build/plugins/lib/test_const
    contrib/python/PyYAML
    library/python/testing/custom_linter_util
    library/python/testing/style
)

PY_SRCS(
    __main__.py
)

STYLE_PYTHON()

END()
