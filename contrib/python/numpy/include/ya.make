SUBSCRIBER(g:python-contrib)

LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(1.25.2)

ADDINCL(
    GLOBAL contrib/python/numpy/include/numpy/core/include
    GLOBAL contrib/python/numpy/include/numpy/core/include/numpy
    GLOBAL contrib/python/numpy/include/numpy/core/src/common
    GLOBAL contrib/python/numpy/include/numpy/core/src/npymath
    GLOBAL contrib/python/numpy/include/numpy/distutils/include
    GLOBAL FOR cython contrib/python/numpy/include/numpy/core/include
    GLOBAL FOR cython contrib/python/numpy/include/numpy/core/include/numpy
)

IF (PYTHON_TYPE_FOR_CYTHON == "PY3")
    ADDINCL(
        GLOBAL FOR cython contrib/python/numpy/py3
    )
ENDIF()

END()
