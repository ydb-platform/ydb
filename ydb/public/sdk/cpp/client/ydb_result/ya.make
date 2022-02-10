LIBRARY()

OWNER(
    dcherednik
    g:kikimr
)

SRCS(
    proto_accessor.cpp
    result.cpp
)

PEERDIR(
    ydb/public/api/protos 
    ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers 
    ydb/public/sdk/cpp/client/ydb_value 
)

END()

RECURSE_FOR_TESTS( 
    ut 
) 
