LIBRARY()

SRCS(
    result_parser.cpp
)

PEERDIR(
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

PROVIDES(YqlResultParser)

END()

RECURSE_FOR_TESTS(ut)
