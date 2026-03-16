PY3_PROGRAM()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/luigi
)

PY_MAIN(luigi.cmdline:luigi_run)

END()
