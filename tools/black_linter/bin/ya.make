PY3_PROGRAM(black_linter)

STYLE_PYTHON()

PEERDIR(
    contrib/python/black
    devtools/ya/yalibrary/term
    library/python/testing/custom_linter_util
)

SRCDIR(
    tools/black_linter/bin
)

PY_SRCS(
    __main__.py
    main2.py
)

END()
