PY3_PROGRAM(certipy)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/certipy
)

NO_LINT()

PY_MAIN(certipy.command_line:main)

END()
