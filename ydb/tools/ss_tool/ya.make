PROGRAM(ss_tool)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/json
    ydb/core/testlib/pg
    ydb/tools/ss_tool/lib
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
    ut
)
