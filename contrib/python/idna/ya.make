PY23_LIBRARY() 

LICENSE(BSD-3-Clause)

OWNER(g:python-contrib)

VERSION(2.10)

NO_LINT()

RESOURCE_FILES(
    PREFIX contrib/python/idna/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

PY_SRCS(
    TOP_LEVEL
    idna/__init__.py
    idna/codec.py
    idna/compat.py
    idna/core.py
    idna/idnadata.py
    idna/intranges.py
    idna/uts46data.py
    idna/package_data.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
