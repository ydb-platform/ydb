LIBRARY() 
 
OWNER( 
    alexnick
    komels 
    g:kikimr 
) 
 
SRCS( 
    jwt.cpp 
    jwt.h 
) 
 
PEERDIR( 
    contrib/libs/jwt-cpp
    library/cpp/json 
    ydb/public/sdk/cpp/client/impl/ydb_internal/common
) 
 
END() 
