LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
