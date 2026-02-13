PY3_LIBRARY()

    PY_SRCS (
        common.py
        publish_metrics.py
        instrumented_client.py
        instrumented_pools.py
    )
    PEERDIR(
        contrib/python/requests
    )

END()
