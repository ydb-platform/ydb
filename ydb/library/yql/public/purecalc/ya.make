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

RECURSE(
    common
    examples
    helpers
    io_specs
)

RECURSE_FOR_TESTS(
    ut
)
