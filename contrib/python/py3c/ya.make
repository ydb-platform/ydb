PY23_NATIVE_LIBRARY(py3c)

LICENSE(MIT)

VERSION(1.4)

SRCS(
    py3c.cpp
)

NO_LINT()

RESOURCE_FILES(
    PREFIX contrib/python/py3c/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()
