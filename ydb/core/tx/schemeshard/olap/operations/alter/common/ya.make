LIBRARY()

SRCS(
    update.cpp
    object.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
)

YQL_LAST_ABI_VERSION()

END()
