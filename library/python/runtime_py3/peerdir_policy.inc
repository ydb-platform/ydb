PEERDIR_FROM_POLICY(
    ALLOW
    contrib/libs/python
    library/python/runtime_py3
    tools/py3cc
    contrib/tools/cython/bin$
    contrib/tools/cython_py2/bin$
)

PEERDIR_FROM_POLICY(
    DENY
    .*
)
