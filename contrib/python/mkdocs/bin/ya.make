PY3_PROGRAM(mkdocs)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PY_MAIN(mkdocs.__main__:cli)

PEERDIR(
    contrib/python/mkdocs
)

END()
