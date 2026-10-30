UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/blobstorage/dsproxy
)

YQL_LAST_ABI_VERSION()

SRCS(
    mirror3dc_blob_tracker_ut.cpp
    mirror3dc_get_ut.cpp
    mirror3dc_put_ut.cpp
    strategy_ut.cpp
)

END()
