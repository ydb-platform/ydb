PY23_LIBRARY()

LICENSE(Apache-2.0)

# there is no external sources
VERSION(Service-proxy-version)

SUBSCRIBER(
    g:yatool
    g:python-contrib
)

PEERDIR(
    build/plugins/lib/test_const
    contrib/tools/cython/Cython
    library/python/testing/coverage_utils
)

PY_SRCS(
    yarcadia/plugin.py
)

RESOURCE(
    coveragerc.txt /coverage_plugins/coveragerc.txt
)

END()
