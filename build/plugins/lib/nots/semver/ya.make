PY23_LIBRARY()

OWNER(g:frontend-build-platform)

PY_SRCS(
    __init__.py
    semver.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
