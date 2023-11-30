PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    devtools/ya/build/build_opts
    devtools/ya/core
    devtools/ya/core/yarg
    devtools/ya/exts
    devtools/ya/package/const
    devtools/ya/test/const
    devtools/ya/test/opts
)

STYLE_PYTHON()

END()
