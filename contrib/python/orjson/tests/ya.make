PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/orjson
    contrib/python/arrow
    contrib/python/psutil
    contrib/python/pytz
    contrib/python/tzdata
    contrib/python/numpy
    contrib/python/pandas
    contrib/python/xxhash
)

ALL_PYTEST_SRCS(RECURSIVE)

END()
