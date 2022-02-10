OWNER( 
    g:kikimr
    g:sqs 
) 
 
LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/public/lib/deprecated/client
)

END()
