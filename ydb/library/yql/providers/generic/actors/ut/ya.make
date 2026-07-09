UNITTEST_FOR(ydb/library/yql/providers/generic/actors)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/formats/arrow/serializer
    ydb/core/kqp/ut/federated_query/common
    ydb/library/actors/testlib
    ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    yql/essentials/minikql
    yql/essentials/minikql/computation/llvm16
    yql/essentials/providers/common/schema/mkql
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    yql_generic_lookup_actor_ut.cpp
    yql_generic_write_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
