SUBSCRIBER(g:frontend_build_platform)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    constants.py
    lockfile.py
    node_modules_bundler.py
    package_json.py
    package_manager.py
    utils.py
)

PEERDIR(
    contrib/python/six
    library/python/archive
)

END()

RECURSE_FOR_TESTS(
    tests
)
