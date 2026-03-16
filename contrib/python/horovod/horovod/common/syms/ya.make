LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/horovod/horovod/common
    library/python/symbols/registry
)

NO_COMPILER_WARNINGS()

SRCS(
    GLOBAL syms.cpp
)

END()
