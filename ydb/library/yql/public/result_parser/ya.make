LIBRARY()

SRCS(
    result_parser.cpp
    result_visitor_base.cpp
)

PEERDIR(
    ydb/library/yql/public/issue
    ydb/library/yql/public/udf
)

YQL_LAST_ABI_VERSION()

PROVIDES(YqlResultParser)

END()

RECURSE_FOR_TESTS(ut)
