LIBRARY()

OWNER(pg)

PEERDIR(
    library/cpp/getopt/small 
    library/cpp/svnversion
    library/cpp/build_info
)

SRCS(
    GLOBAL print.cpp 
)

END()
