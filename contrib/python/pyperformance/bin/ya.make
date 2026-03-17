PY3_PROGRAM(pyperformance)

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/python/pyperformance
)

PY_MAIN(pyperformance.cli:main)

END()
