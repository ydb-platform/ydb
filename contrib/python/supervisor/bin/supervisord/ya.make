PY3_PROGRAM(supervisord)

VERSION(Service-proxy-version)

LICENSE(BSD)

PEERDIR(
    contrib/python/supervisor
)

PY_MAIN(supervisor.supervisord)

END()
