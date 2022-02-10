PROGRAM()

OWNER(g:kikimr) 

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/core/tablet_flat
)

END()
