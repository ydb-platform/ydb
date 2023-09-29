PY2_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(1.1.0)

NO_LINT()

PY_SRCS(
    TOP_LEVEL
    itsdangerous/_compat.py
    itsdangerous/encoding.py
    itsdangerous/exc.py
    itsdangerous/__init__.py
    itsdangerous/_json.py
    itsdangerous/jws.py
    itsdangerous/serializer.py
    itsdangerous/signer.py
    itsdangerous/timed.py
    itsdangerous/url_safe.py
)

RESOURCE_FILES(
    PREFIX contrib/python/itsdangerous/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()

RECURSE_FOR_TESTS(
    tests
)
