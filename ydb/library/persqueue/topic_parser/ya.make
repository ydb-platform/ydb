OWNER(
    komels
    g:kikimr
    g:logbroker
)

LIBRARY()

PEERDIR(
    ydb/core/base 
    ydb/library/persqueue/topic_parser_public 
    ydb/public/api/protos 
)

SRCS(
    topic_parser.h
    topic_parser.cpp
)
 
END()

RECURSE_FOR_TESTS( 
    ut
)
