PY3_PROGRAM(cffigen)

OWNER(orivej)

DISABLE(PYTHON_SQLITE3)

PEERDIR(
    contrib/python/cffi/gen/lib
)

INDUCED_DEPS(cpp ${ARCADIA_ROOT}/contrib/python/cffi/cffi/_cffi_include.h) 

END()

RECURSE(
    lib
)
