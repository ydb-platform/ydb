PY23_LIBRARY()

STYLE_PYTHON()

PY_CONSTRUCTOR(library.python.import_tracing.constructor)

PY_SRCS(
    __init__.py
)

PEERDIR(
    library/python/import_tracing/lib
)

END()
