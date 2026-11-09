UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    cms_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/cms
)

END()
