LIBRARY()

PROVIDES(python)

VERSION(Service-proxy-version)

LICENSE(Python-2.0)

PEERDIR(
    contrib/tools/python3
    contrib/tools/python3/Lib
)

SUPPRESSIONS(lsan.supp)

END()
