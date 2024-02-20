OWNER(g:frontend-build-platform)

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
