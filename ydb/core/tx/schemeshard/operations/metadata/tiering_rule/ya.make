LIBRARY()

SRCS(
    object.cpp
    update.cpp
)

PEERDIR(
    # ydb/core/tx/schemeshard
    # ydb/core/tx/schemeshard/operations/metadata/abstract
    ydb/core/tx/schemeshard/operations/abstract
)

YQL_LAST_ABI_VERSION()

END()
