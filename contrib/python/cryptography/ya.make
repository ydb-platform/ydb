PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

IF (PYTHON2)
    PEERDIR(contrib/python/cryptography/py2)
ELSE()
    IF (ARCH_X86_64 AND OS_LINUX OR ARCH_X86_64 AND OS_DARWIN)
        PEERDIR(contrib/python/cryptography/next)
    ELSE()
        PEERDIR(contrib/python/cryptography/py3)
    ENDIF()
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)

IF (OS_LINUX AND MUSL)
    RECURSE(next)
ENDIF()
