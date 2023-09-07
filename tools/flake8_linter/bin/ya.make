PY3_PROGRAM(flake8_linter)

STYLE_PYTHON()

PEERDIR(
    build/plugins/lib/test_const
    devtools/ya/test/programs/test_tool/lib/migrations_config
    devtools/ya/yalibrary/term
    library/python/testing/custom_linter_util
)

SRCDIR(
    tools/flake8_linter/bin
)

PY_SRCS(
    __main__.py
)

END()
