PY3_PROGRAM(pgcli)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/pgcli
)

NO_LINT()

PY_MAIN(
    pgcli.main:cli
)

END()
