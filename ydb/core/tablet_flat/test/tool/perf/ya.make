PROGRAM(table-perf)

OWNER(g:kikimr)

SRCS(
    colons.cpp
    main.cpp
)

PEERDIR(
    ydb/core/tablet_flat/test/libs/table
    library/cpp/charset
    library/cpp/getopt
    ydb/core/tablet_flat
)

END()
