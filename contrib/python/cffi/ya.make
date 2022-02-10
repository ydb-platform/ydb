PY23_LIBRARY() 

LICENSE(MIT) 
 
OWNER(g:python-contrib)

VERSION(1.15.0)

PEERDIR(
    contrib/restricted/libffi
    contrib/python/pycparser
)

ADDINCL(
    contrib/restricted/libffi/include
)

NO_COMPILER_WARNINGS()
NO_LINT()

SUPPRESSIONS(lsan.supp)

SRCS(
    c/_cffi_backend.c
)

PY_REGISTER(_cffi_backend)

PY_SRCS(
    TOP_LEVEL
    cffi/__init__.py
    cffi/api.py
    cffi/backend_ctypes.py
    cffi/cffi_opcode.py
    cffi/commontypes.py
    cffi/cparser.py
    cffi/error.py
    cffi/ffiplatform.py
    cffi/lock.py
    cffi/model.py
    cffi/pkgconfig.py
    cffi/recompiler.py
    cffi/setuptools_ext.py
    cffi/vengine_cpy.py
    cffi/vengine_gen.py
    cffi/verifier.py
)

RESOURCE_FILES(
    PREFIX contrib/python/cffi/
    .dist-info/METADATA
    .dist-info/entry_points.txt
    .dist-info/top_level.txt
    cffi/_cffi_errors.h
    cffi/_cffi_include.h
    cffi/_embedding.h
    cffi/parse_c_type.h
)

END()
