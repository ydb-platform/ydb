LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    ydb_dynamic_config.h
    ydb_replication.h
    ydb_scripting.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/draft
)

END()
