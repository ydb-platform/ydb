PY3_LIBRARY()

    SUPPRESSIONS(
        tsan.supp
    )

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
