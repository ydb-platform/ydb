PY3_LIBRARY()

    PY_SRCS (
        healthcheck_reporter.py
    )

    PEERDIR (
        contrib/python/requests
        library/python/resource
    )

    BUNDLE(
        ydb/apps/ydb NAME ydb_cli
    )

    RESOURCE(ydb_cli ydb_cli)
END()
