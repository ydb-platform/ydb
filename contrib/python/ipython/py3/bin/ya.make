PY3_PROGRAM(ipython)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/ipython
)

PY_MAIN(IPython:start_ipython)

END()
