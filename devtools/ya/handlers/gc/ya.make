PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    NAMESPACE handlers.gc
    __init__.py
)

PEERDIR(
    contrib/python/six
    devtools/ya/app
    devtools/ya/build
    devtools/ya/build/build_opts
    devtools/ya/core
    devtools/ya/core/config
    devtools/ya/core/yarg
    devtools/ya/exts
    devtools/ya/yalibrary/runner
    devtools/ya/yalibrary/store
    devtools/ya/yalibrary/toolscache
    library/python/fs
    library/python/windows
)

IF (NOT YA_OPENSOURCE)
    PEERDIR(
        devtools/ya/yalibrary/store/yt_store # see YA-938
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    bin
    tests
)
