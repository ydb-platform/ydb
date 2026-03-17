PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

IF (PYTHON2)
    PEERDIR(contrib/python/antlr4/antlr4-python2-runtime)
ELSE()
    PEERDIR(contrib/python/antlr4/antlr4-python3-runtime)
ENDIF()

END()

RECURSE(
    antlr4-python2-runtime
    antlr4-python3-runtime
)
