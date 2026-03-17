PY3_PROGRAM()

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/pyserial
)

PY_MAIN(serial.tools.miniterm:main)

NO_LINT()

END()

