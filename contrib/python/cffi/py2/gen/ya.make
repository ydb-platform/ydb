PY2_PROGRAM(cffigen)

DISABLE(PYTHON_SQLITE3)

LICENSE(MIT)

PEERDIR(
    contrib/python/cffi/py2/gen/lib
)

INDUCED_DEPS(cpp ${ARCADIA_ROOT}/contrib/python/cffi/py2/cffi/_cffi_include.h)

END()

RECURSE(
    lib
)
