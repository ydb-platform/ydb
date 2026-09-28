PROGRAM()

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    ydb/core/base
    ydb/core/kesus/tablet
    ydb/core/quoter
    ydb/core/testlib/default
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
    quota_requester.cpp
    server.cpp
)

END()
