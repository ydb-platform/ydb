LIBRARY()

SRCS(
    db_schema.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
