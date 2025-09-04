PY3_PROGRAM()

STYLE_PYTHON()

PEERDIR(
    build/plugins/lib/test_const
    library/python/testing/custom_linter_util
    library/python/testing/style
)

PY_SRCS(
    wrapper.py
)

END()
