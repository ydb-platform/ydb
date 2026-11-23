YQL_LIBRARY()

SRCS(
    update.cpp
    object.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
