PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    NAMESPACE handlers.tool
    __init__.py
)

PEERDIR(
    devtools/ya/app
    devtools/ya/build/build_opts
    devtools/ya/core
    devtools/ya/core/config
    devtools/ya/core/respawn
    devtools/ya/core/yarg
    devtools/ya/exts
    devtools/ya/yalibrary/platform_matcher
    devtools/ya/yalibrary/tools
    devtools/ya/yalibrary/toolscache
    library/python/windows
)

END()

RECURSE(
    tests
)

RECURSE_FOR_TESTS(
    bin
    tests
)
