LIBRARY() 
 
OWNER( 
    galaxycrab 
    g:kikimr 
) 
 
SRCS( 
    hide_field_printer.cpp 
    stream_helper.cpp 
    token_field_printer.cpp 
) 
 
PEERDIR( 
    contrib/libs/protobuf 
    ydb/library/security
    ydb/public/api/protos/annotations
) 
 
END() 
 
RECURSE_FOR_TESTS( 
    ut 
) 
