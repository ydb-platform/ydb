PY2_LIBRARY()

VERSION(0.10.1)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/toolz
)

ADDINCL(
    contrib/python/cytoolz/py2
    FOR cython contrib/python/cytoolz/py2
)

NO_LINT()

PY_SRCS(
    TOP_LEVEL
    cytoolz/__init__.py
    cytoolz/_signatures.py
    cytoolz/_version.py
    cytoolz/compatibility.py
    cytoolz/curried/__init__.py
    cytoolz/curried/exceptions.py
    cytoolz/curried/operator.py
    cytoolz/dicttoolz.pyx
    cytoolz/functoolz.pyx
    cytoolz/itertoolz.pyx
    cytoolz/recipes.pyx
    cytoolz/utils.pyx
)

RESOURCE_FILES(
    PREFIX contrib/python/cytoolz/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()
