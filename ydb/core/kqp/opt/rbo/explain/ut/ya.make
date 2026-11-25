UNITTEST()

SRCS(
    kqp_read_ranges_ut.cpp
)

PEERDIR(
    ydb/core/kqp/opt/rbo
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
