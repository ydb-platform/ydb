PROGRAM()

OWNER(
    galaxycrab
    g:kikimr
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    ydb/core/base
    ydb/core/kesus/tablet
    ydb/core/quoter
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
    quota_requester.cpp
    server.cpp
)

END()
