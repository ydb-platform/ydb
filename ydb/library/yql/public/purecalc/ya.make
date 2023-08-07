LIBRARY()

SRCS(
    purecalc.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/public/purecalc/common
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    io_specs/ut
    ut
)
