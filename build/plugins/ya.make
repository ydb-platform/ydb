OWNER(g:ymake)

PY2_LIBRARY()

PY_NAMESPACE(.)

PY_SRCS(
    code_generator.py
    ssqls.py
    maps_mobile_idl.py

    _common.py
    _requirements.py
)

PEERDIR(
    build/plugins/lib/proxy
    build/plugins/lib/test_const/proxy
)

END()

RECURSE(
    tests
    lib
    lib/proxy
    lib/test_const
    lib/test_const/proxy
)
