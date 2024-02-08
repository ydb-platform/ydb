PY2_LIBRARY()

LICENSE(PSF-2.0)

NO_COMPILER_WARNINGS()
NO_LINT()

PEERDIR(
    ADDINCL contrib/python/numpy
)

ADDINCL(
    contrib/python/matplotlib/py2
)

CFLAGS(
    -DPY_ARRAY_UNIQUE_SYMBOL=MPL_matplotlib__tri_ARRAY_API
    -DNPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
)

PY_SRCS(
    NAMESPACE matplotlib.tri
    __init__.py
    triangulation.py
    tricontour.py
    trifinder.py
    triinterpolate.py
    tripcolor.py
    triplot.py
    trirefine.py
    tritools.py
)

PY_REGISTER(matplotlib._tri)

SRCS(
    _tri.cpp
    _tri_wrapper.cpp
)

END()
