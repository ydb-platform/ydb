LIBRARY()

OWNER(
    chertus
    g:kikimr
)

SRCS(
    import.cpp 
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/ydb_proto
)

END()
