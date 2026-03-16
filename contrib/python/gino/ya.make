PY3_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(1.0.1)

PEERDIR(
    contrib/python/asyncpg
    library/python/pyscopg2
    contrib/python/sqlalchemy/sqlalchemy-1.3
    contrib/python/importlib-metadata
)

NO_LINT()

NO_CHECK_IMPORTS(
    gino.ext.starlette
)

PY_SRCS(
    TOP_LEVEL
    gino/__init__.py
    gino/aiocontextvars.py
    gino/api.py
    gino/crud.py
    gino/declarative.py
    gino/dialects/__init__.py
    gino/dialects/asyncpg.py
    gino/dialects/base.py
    gino/engine.py
    gino/exceptions.py
    gino/ext/__init__.py
    gino/ext/starlette.py
    gino/json_support.py
    gino/loader.py
    gino/schema.py
    gino/strategies.py
    gino/transaction.py
)

RESOURCE_FILES(
    PREFIX contrib/python/gino/
    .dist-info/METADATA
    .dist-info/entry_points.txt
    .dist-info/top_level.txt
    gino/ext/py.typed
)

END()
