PY3_LIBRARY()

PEERDIR(
    contrib/python/aiohttp
    library/python/monlib
)

PY_SRCS(
    config.py
    multi_shard.py
    shard.py
    webapp.py
)

END()
