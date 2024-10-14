LIBRARY()

SRCS(
    update.cpp
    object.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/operations/abstract
    ydb/core/tx/schemeshard/operations/metadata/tiering_rule
)

YQL_LAST_ABI_VERSION()

END()
