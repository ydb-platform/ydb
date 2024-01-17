PY3_LIBRARY()

VERSION(1.16)

LICENSE(MIT)

NO_LINT()

PY_SRCS(
    TOP_LEVEL
    patch.py
)

RESOURCE_FILES(
    PREFIX contrib/python/patch/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()
