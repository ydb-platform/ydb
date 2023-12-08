# Edited to peerdir certifi, dispatch between py2 and py3, remove
# certs.txt.

PY2_LIBRARY()

LICENSE(MIT)

VERSION(0.20.4)

NO_LINT()

PEERDIR(
    contrib/python/certifi
    contrib/python/pyparsing
)

PY_SRCS(
    TOP_LEVEL
    httplib2/__init__.py
    httplib2/auth.py
    httplib2/certs.py
    httplib2/error.py
    httplib2/iri2uri.py
    httplib2/socks.py
)

RESOURCE_FILES(
    PREFIX contrib/python/httplib2/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()
