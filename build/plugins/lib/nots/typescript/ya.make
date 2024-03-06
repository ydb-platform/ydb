OWNER(g:frontend-build-platform)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    ts_errors.py
    ts_glob.py
    ts_config.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager
)

IF (PYTHON3)
    PEERDIR(
        contrib/python/python-rapidjson
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    tests
)
