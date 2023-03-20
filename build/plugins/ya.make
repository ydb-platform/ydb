OWNER(g:ymake)

PY2_LIBRARY()

PY_SRCS(
    code_generator.py
    ssqls.py
    maps_mobile_idl.py

    _common.py
    _requirements.py
)

PEERDIR(
    build/plugins/lib
    build/plugins/lib/test_const
)

END()

RECURSE(
    tests
    lib/test_const
)
