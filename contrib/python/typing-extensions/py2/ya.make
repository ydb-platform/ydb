PY2_LIBRARY()

LICENSE(PSF-2.0)

VERSION(3.10.0.2)

NO_LINT()

PEERDIR(
    contrib/deprecated/python/typing
)

SRCDIR(
    contrib/python/typing-extensions/py2/src_py2
)

PY_SRCS(
    TOP_LEVEL
    typing_extensions.py
)

RESOURCE_FILES(
    PREFIX contrib/python/typing-extensions/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()

RECURSE_FOR_TESTS(
    tests
)
