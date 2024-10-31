LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/common
    ydb/core/tx/schemeshard/operations/abstract
)

YQL_LAST_ABI_VERSION()

END()
