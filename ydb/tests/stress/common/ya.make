PY3_LIBRARY()

    PY_SRCS (
        common.py
        publish_metrics.py
    )
    PEERDIR(
        contrib/python/requests
    )

END()
