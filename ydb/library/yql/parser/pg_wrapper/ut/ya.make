UNITTEST_FOR(ydb/library/yql/parser/pg_wrapper)

OWNER(g:yql)

TIMEOUT(600)
SIZE(MEDIUM)

SRCS(
    wrapper_ut.cpp
)

END()
