PY2_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(3.1.2.post1)

PEERDIR(
    contrib/libs/libarchive
    contrib/python/contextlib2
    contrib/python/six
)

ADDINCL(
    contrib/libs/libarchive/libarchive
)

NO_LINT()

PY_SRCS(
    SWIG_C
    TOP_LEVEL
    libarchive/__init__.py
    libarchive/tar.py
    libarchive/zip.py
    libarchive/_libarchive.swg
)

END()
