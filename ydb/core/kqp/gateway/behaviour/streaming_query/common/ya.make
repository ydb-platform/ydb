LIBRARY()

SRCS(
    utils.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    yql/essentials/sql/v1
)

YQL_LAST_ABI_VERSION()

END()
