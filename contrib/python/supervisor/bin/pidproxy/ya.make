PY3_PROGRAM(pidproxy)

VERSION(Service-proxy-version)

LICENSE(BSD)

PEERDIR(
    contrib/python/supervisor
)

PY_MAIN(supervisor.pidproxy)

END()
