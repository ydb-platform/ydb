UNITTEST()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/sys_view/show_create/formatters
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    create_table_formatter_ut.cpp
)

END()
