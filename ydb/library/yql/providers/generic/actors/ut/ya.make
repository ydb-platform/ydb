UNITTEST_FOR(ydb/library/yql/providers/generic/actors)

PEERDIR(
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    ydb/library/actors/testlib
    library/cpp/testing/unittest
)

SRCS(
    yql_generic_lookup_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
