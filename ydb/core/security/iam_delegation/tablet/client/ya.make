LIBRARY()
SRCS(client.cpp)
PEERDIR(
    ydb/core/base
    ydb/core/security/iam_delegation/tablet/public
    ydb/library/actors/core
)
END()
