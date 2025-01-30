LIBRARY()

SRCS(
    metadata_helpers.cpp
    scheme_helpers.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/provider
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
