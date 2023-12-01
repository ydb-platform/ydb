PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    NAMESPACE handlers.java
    __init__.py
    helpers.py
)

PEERDIR(
    devtools/ya/app
    devtools/ya/core/yarg
    devtools/ya/build
    devtools/ya/build/build_opts
    devtools/ya/test/opts
)

END()

RECURSE_FOR_TESTS(
    bin
    tests
)
