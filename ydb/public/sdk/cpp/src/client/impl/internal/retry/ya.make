LIBRARY()

SRCS(
    bulk_upsert_retry_state.cpp
    retry.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/observability
    ydb/public/sdk/cpp/src/client/value
)

END()
