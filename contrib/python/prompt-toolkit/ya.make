PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

OWNER(g:python-contrib)

IF (PYTHON2)
    PEERDIR(contrib/python/prompt-toolkit/py2)
ELSE()
    PEERDIR(contrib/python/prompt-toolkit/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)
