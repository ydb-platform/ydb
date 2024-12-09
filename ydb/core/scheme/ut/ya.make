UNITTEST_FOR(ydb/core/scheme)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    ydb/core/scheme
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    scheme_borders_ut.cpp
    scheme_ranges_ut.cpp
    scheme_tablecell_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
