LIBRARY()

SRCS(
    update.cpp
    object.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/operations/abstract
)

YQL_LAST_ABI_VERSION()

END()
