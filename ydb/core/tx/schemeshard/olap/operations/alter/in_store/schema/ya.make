LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/tiering/rule
)

YQL_LAST_ABI_VERSION()

END()
