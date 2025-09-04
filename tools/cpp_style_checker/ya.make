PY3_PROGRAM()

PEERDIR(
    build/plugins/lib/test_const
    library/python/testing/custom_linter_util
    library/python/testing/style
)

PY_SRCS(
    wrapper.py
)

STYLE_PYTHON()

END()
