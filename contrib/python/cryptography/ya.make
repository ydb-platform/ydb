PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

IF (PYTHON2)
    PEERDIR(contrib/python/cryptography/py2)
ELSE()
    IF (OS_LINUX AND ARCH_X86_64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_DARWIN AND ARCH_ARM64)
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
