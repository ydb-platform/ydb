PROGRAM(pdisktool)

PEERDIR(
    library/cpp/getopt
    ydb/apps/version
    ydb/tools/pdisktool/lib
)

SRCS(
    main.cpp
)

END()

RECURSE(
    lib
    proto
)

RECURSE_FOR_TESTS(
    ut
)
