PY3_LIBRARY()

PEERDIR(
    contrib/python/aiohttp
    contrib/python/grpcio
    
    library/python/monlib
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common

    ydb/library/yql/providers/solomon/solomon_accessor/grpc
)

PY_SRCS(
    config.py
    multi_shard.py
    shard.py
    webapp.py
)

END()
