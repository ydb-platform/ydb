UNITTEST_FOR(ydb/library/yql/providers/generic/actors)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/kqp/ut/federated_query/common
    ydb/library/actors/testlib
    ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    yql/essentials/sql/pg_dummy
)

IF (NOT OS_WINDOWS)
SRCS(
    yql_generic_lookup_actor_ut.cpp
)
ELSE()
# TTestActorRuntimeBase(..., true) seems broken on windows
ENDIF()

YQL_LAST_ABI_VERSION()

END()
