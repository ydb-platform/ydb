UNITTEST_FOR(library/cpp/monlib/encode/prometheus) 
 
OWNER( 
    g:solomon 
    jamel 
) 
 
SRCS( 
    prometheus_encoder_ut.cpp 
    prometheus_decoder_ut.cpp 
) 
 
PEERDIR( 
    library/cpp/monlib/encode/protobuf 
) 
 
END() 
