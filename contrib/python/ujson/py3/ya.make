PY3_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(1.35+dev)

NO_COMPILER_WARNINGS()
NO_UTIL()

PY_REGISTER(ujson)

ADDINCL(
    contrib/python/ujson/py3/lib
    contrib/python/ujson/py3/python
)

SRCS(
    lib/ultrajsondec.c
    lib/ultrajsonenc.c
    python/JSONtoObj.c
    python/objToJSON.c
    python/ujson.c
)

PY_SRCS(
    TOP_LEVEL
    ujson.pyi
)

END()
