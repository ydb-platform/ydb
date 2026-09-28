SUBSCRIBER(g:frontend_build_platform)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    common_config.py
    constants.py
    lockfile.py
    package_json.py
    package_manager.py
    timeit.py
    utils.py
)

PEERDIR(
    contrib/python/PyYAML
    devtools/frontend_build_platform/libraries/logging
)

END()

RECURSE_FOR_TESTS(
    tests
)
