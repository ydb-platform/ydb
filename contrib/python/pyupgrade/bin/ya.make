PY3_PROGRAM(pyupgrade)

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/python/pyupgrade
)

PY_MAIN(pyupgrade._main:main)

END()
