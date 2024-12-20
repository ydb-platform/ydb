PY3_PROGRAM(coverage)

SUBSCRIBER(g:yatool)

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/coverage
)

PY_MAIN(coverage.cmdline:main)

END()
