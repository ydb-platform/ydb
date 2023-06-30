RECURSE(
    clickhouse
    common
    config
    dq
    pq
    result
    s3
    solomon
    ydb
    function
    generic
)


# TODO(max42): Recurse unconditionally as a final step of YT-19210.
IF (NOT EXPORT_CMAKE)
    RECURSE(
    	yt
    	stat
    )
ENDIF()
