PY3_PROGRAM(flake8_linter)

IF (NOT USE_SYSTEM_PYTHON)

STYLE_PYTHON()

PEERDIR(
    build/plugins/lib/test_const
    devtools/ya/test/programs/test_tool/lib/migrations_config
    library/python/testing/custom_linter_util
)

PY_SRCS(
    __main__.py
)

ENDIF()
END()

IF (NOT USE_SYSTEM_PYTHON AND NOT OPENSOURCE)
RECURSE_FOR_TESTS(
    tests
)
ENDIF()
