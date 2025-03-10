LIBRARY()

SRCS(
    schema.cpp
    update.cpp
    validator.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tx/tiering/tier
)

YQL_LAST_ABI_VERSION()

END()
