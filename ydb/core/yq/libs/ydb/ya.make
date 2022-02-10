OWNER(g:yq)
 
LIBRARY() 
 
SRCS( 
    util.cpp 
    ydb.cpp 
) 
 
PEERDIR( 
    ydb/core/yq/libs/config
    ydb/library/security
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
) 
 
GENERATE_ENUM_SERIALIZATION(ydb.h) 
 
END() 
