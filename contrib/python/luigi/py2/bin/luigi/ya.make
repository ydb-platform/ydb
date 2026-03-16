PY2_PROGRAM()

LICENSE(Apache-2.0)

VERSION(Service-proxy-version)

PEERDIR(
    contrib/python/luigi
)

PY_MAIN(luigi.cmdline:luigi_run)

END()
