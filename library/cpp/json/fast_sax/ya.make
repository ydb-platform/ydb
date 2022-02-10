LIBRARY()

OWNER( 
    pg 
    velavokr 
) 

PEERDIR(
    library/cpp/json/common
)

SRCS(
    parser.rl6
    unescape.cpp
)

END()
