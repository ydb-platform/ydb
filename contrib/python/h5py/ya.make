PY3_LIBRARY()

VERSION(3.13.0)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/libs/hdf5/hl/src
    contrib/libs/hdf5/src
    contrib/python/numpy
)

ADDINCL(
    contrib/libs/hdf5/hl/src
    contrib/libs/hdf5/src
    contrib/python/h5py/h5py
    contrib/python/h5py/lzf
    contrib/python/h5py/lzf/lzf
    FOR cython contrib/python/mpi4py
    FOR cython contrib/python/numpy/py3
)

NO_COMPILER_WARNINGS()

NO_LINT()

NO_CHECK_IMPORTS(
    h5py.ipy_completer
)

CFLAGS(
    -DH5_USE_110_API
    -DH5Rdereference_vers=2
    -DNPY_NO_DEPRECATED_API=0
)

SRCS(
    lzf/lzf/lzf_c.c
    lzf/lzf/lzf_d.c
    lzf/lzf_filter.c
)

PY_SRCS(
    TOP_LEVEL
    h5py/__init__.py
    h5py/_hl/__init__.py
    h5py/_hl/attrs.py
    h5py/_hl/base.py
    h5py/_hl/compat.py
    h5py/_hl/dataset.py
    h5py/_hl/datatype.py
    h5py/_hl/dims.py
    h5py/_hl/files.py
    h5py/_hl/filters.py
    h5py/_hl/group.py
    h5py/_hl/selections.py
    h5py/_hl/selections2.py
    h5py/_hl/vds.py
    h5py/h5py_warnings.py
    h5py/ipy_completer.py
    h5py/version.py
    CYTHON_C
    h5py/_conv.pyx
    h5py/_errors.pyx
    h5py/_objects.pyx
    h5py/_proxy.pyx
    h5py/_selector.pyx
    h5py/defs.pyx
    h5py/h5.pyx
    h5py/h5a.pyx
    h5py/h5ac.pyx
    h5py/h5d.pyx
    h5py/h5ds.pyx
    h5py/h5f.pyx
    h5py/h5fd.pyx
    h5py/h5g.pyx
    h5py/h5i.pyx
    h5py/h5l.pyx
    h5py/h5o.pyx
    h5py/h5p.pyx
    h5py/h5pl.pyx
    h5py/h5r.pyx
    h5py/h5s.pyx
    h5py/h5t.pyx
    h5py/h5z.pyx
    h5py/utils.pyx
)

RESOURCE_FILES(
    PREFIX contrib/python/h5py/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()
