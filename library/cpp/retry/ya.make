LIBRARY()

OWNER( 
    osado 
    g:yabs-small 
) 

SRCS(
    retry.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/retry/protos
)

END()
