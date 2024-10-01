LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    table_enum.h
    table.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/table
)

END()
