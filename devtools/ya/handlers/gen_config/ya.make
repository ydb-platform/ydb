PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    NAMESPACE handlers.gen_config
    __init__.py
    gen_config.py
)

PEERDIR(
    contrib/python/six
    contrib/python/toml
    devtools/ya/core
    devtools/ya/core/yarg
)

END()

RECURSE_FOR_TESTS(
    bin
    tests
)
