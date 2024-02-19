OWNER(g:frontend-build-platform)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    constants.py
    lockfile.py
    package_manager.py
    workspace.py
    utils.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
    contrib/python/PyYAML
    contrib/python/six
)

END()

RECURSE_FOR_TESTS(
    tests
)
