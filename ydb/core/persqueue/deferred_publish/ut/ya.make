UNITTEST()

SIZE(SMALL)

PEERDIR(
    ydb/core/protos
)

SRCS(
    deferred_publish_destination_blob_ut.cpp
    ../destination_blob.cpp
)

YQL_LAST_ABI_VERSION()

END()
