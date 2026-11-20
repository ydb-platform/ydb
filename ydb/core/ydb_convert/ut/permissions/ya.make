UNITTEST()

YQL_LAST_ABI_VERSION()

SRCS(
    permissions_ut.cpp
)

PEERDIR(
    ydb/core/ydb_convert
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
)

END()
