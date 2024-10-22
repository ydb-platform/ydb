LIBRARY()

SRCS(
    object.cpp
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/operations/abstract
)

YQL_LAST_ABI_VERSION()

END()
