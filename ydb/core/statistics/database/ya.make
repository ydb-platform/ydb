LIBRARY()

SRCS(
    database.h
    database.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    yql/essentials/core/minsketch
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
