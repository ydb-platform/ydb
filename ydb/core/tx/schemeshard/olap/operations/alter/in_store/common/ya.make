LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
)

YQL_LAST_ABI_VERSION()

END()
