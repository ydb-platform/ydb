OWNER( 
    g:kikimr
    g:sqs 
) 
 
LIBRARY()

SRCS(
    queries.cpp
    schema.cpp
)

PEERDIR(
    ydb/core/ymq/base
)

END()
