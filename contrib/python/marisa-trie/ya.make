PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

IF (PYTHON2)
    PEERDIR(contrib/python/marisa-trie/py2)
ELSE()
    PEERDIR(contrib/python/marisa-trie/py3)
ENDIF()

NO_LINT()

END()

IF (NOT OPENSOURCE)
    RECURSE(
        py2
        py3
    )
ENDIF()
