PY3_PROGRAM()

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/numpy
)

PY_SRCS(
    NAMESPACE numpy.f2py
    __init__.py
    __main__.py
    __version__.py
    _backends/__init__.py
    _backends/_backend.py
    _backends/_distutils.py
    _backends/_meson.py
    _isocbind.py
    auxfuncs.py
    capi_maps.py
    cb_rules.py
    cfuncs.py
    common_rules.py
    crackfortran.py
    diagnose.py
    f2py2e.py
    f90mod_rules.py
    func2subr.py
    rules.py
    symbolic.py
    use_rules.py
)

NO_LINT()

END()
