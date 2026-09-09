PROGRAM()

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    library/cpp/testing/common
    ydb/core/base
    ydb/core/kesus/tablet
    ydb/core/quoter
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
    quota_requester.cpp
    server.cpp
)

END()
