YQL_LIBRARY()

SRCS(
    scan_factory.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/core/scheme
    ydb/library/aclib
    ydb/library/actors/core
    yql/essentials/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)
