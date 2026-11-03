LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/tablet_flat
    ydb/core/security/iam_delegation/tablet/public
)

SRCS(
    tablet.cpp
    tx_request.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    protos
    public
)

RECURSE_FOR_TESTS(
    ut
)
