UNITTEST_FOR(library/cpp/grpc/server) 

OWNER(
    dcherednik
    g:kikimr
)

TIMEOUT(600) 
SIZE(MEDIUM) 

PEERDIR(
    library/cpp/grpc/server 
)

SRCS(
    grpc_response_ut.cpp 
    stream_adaptor_ut.cpp
)

END()

