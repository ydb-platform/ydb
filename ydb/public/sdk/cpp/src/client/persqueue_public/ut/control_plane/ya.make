UNITTEST_FOR(ydb/public/sdk/cpp/src/client/persqueue_public)

SRCS(
    replication_attributes_ut.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/persqueue_public/impl
)

END()
