LIBRARY()

OWNER( 
    jamel 
    g:solomon 
) 

SRCS(
    protobuf_encoder.cpp
)

PEERDIR(
    library/cpp/monlib/encode
    library/cpp/monlib/encode/protobuf/protos
)

END()
