LIBRARY()

OWNER(
    prettysimple
    g:kikimr
)

SRCS(
    util.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_types/status
)

END() 
