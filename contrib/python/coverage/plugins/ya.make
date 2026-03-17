PY23_LIBRARY()

LICENSE(Apache-2.0)

# there is no external sources
VERSION(Service-proxy-version)

PEERDIR(
    build/plugins/lib/test_const
    library/python/testing/coverage_utils
)

IF (PYTHON2)
    PEERDIR(contrib/tools/cython_py2/Cython)
    RESOURCE(
        coveragerc_py2.txt /coverage_plugins/coveragerc.txt
    )
ELSE()
    PEERDIR(contrib/tools/cython/Cython)
    RESOURCE(
        coveragerc.txt /coverage_plugins/coveragerc.txt
    )
ENDIF()

PY_SRCS(
    yarcadia/plugin.py
)

END()
