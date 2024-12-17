SUBSCRIBER(g:frontend_build_platform)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    semver.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
