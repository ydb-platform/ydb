PY3_PROGRAM(ipython)

OWNER(g:python-contrib borman nslus)

PEERDIR(
    contrib/python/ipython
)

PY_MAIN(IPython:start_ipython)

END()
