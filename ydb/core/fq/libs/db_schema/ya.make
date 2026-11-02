LIBRARY()

SRCS(
    db_schema.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/table
)

END()
