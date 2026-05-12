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
    pnpm_workspace.py
    timeit.py
    utils.py
)

PEERDIR(
    library/python/archive
    contrib/python/PyYAML
    devtools/frontend_build_platform/libraries/logging
)

END()

RECURSE_FOR_TESTS(
    tests
)
