LIBRARY()

OWNER( 
    g:util 
    jamel 
) 

SRCS(
    histogram.cpp
    histogram_iter.cpp
)

PEERDIR(
    contrib/libs/hdr_histogram
)

END()
