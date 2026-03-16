PY3_PROGRAM(supervisorctl)

VERSION(Service-proxy-version)

LICENSE(BSD)

PEERDIR(
    contrib/python/supervisor
)

PY_MAIN(supervisor.supervisorctl)

END()
