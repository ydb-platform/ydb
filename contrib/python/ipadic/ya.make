PY3_LIBRARY()

LICENSE(MIT)

VERSION(1.0.0)

PY_SRCS(
    NAMESPACE ipadic
    __init__.py
)

FROM_SANDBOX(1932990217 OUT_NOAUTO char.bin dicrc matrix.bin mecabrc sys.dic unk.dic version)

RESOURCE_FILES(
    PREFIX contrib/python/ipadic/
    .dist-info/METADATA
    .dist-info/top_level.txt
    char.bin
    dicrc
    matrix.bin
    mecabrc
    sys.dic
    unk.dic
    version
)

END()
