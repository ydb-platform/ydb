LIBRARY()

PEERDIR(
    contrib/libs/jwt-cpp 
    contrib/libs/protobuf
    library/cpp/digest/argonish
    library/cpp/string_utils/base64
    ydb/library/login/protos 
)

OWNER( 
    xenoxeno 
    g:kikimr 
) 

SRCS(
    login.cpp
    login.h
)
 
END()
 
RECURSE_FOR_TESTS( 
    ut 
) 
