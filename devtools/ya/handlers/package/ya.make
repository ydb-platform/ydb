PY23_LIBRARY()

PY_SRCS(
    NAMESPACE handlers.package
    __init__.py
)

PEERDIR(
    contrib/python/pathlib2
    devtools/ya/app
    devtools/ya/build
    devtools/ya/build/build_opts
    devtools/ya/core
    devtools/ya/core/yarg
    devtools/ya/handlers/package/opts
    devtools/ya/package
    devtools/ya/test/opts
)

STYLE_PYTHON()

END()

RECURSE_FOR_TESTS(
    bin
    opts
)
