UNITTEST_FOR(ydb/library/yql/providers/yt/actors)

PEERDIR(
    ydb/library/yql/providers/yt/codec/codegen/no_llvm
    ydb/library/yql/providers/yt/comp_nodes/no_llvm
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/minikql/codegen/no_llvm
    ydb/library/actors/testlib
    ydb/library/yql/public/udf
    library/cpp/testing/unittest
    ydb/library/yql/sql/pg
    ydb/library/yql/public/udf/service/terminate_policy
    ydb/library/yql/parser/pg_wrapper/interface

)

SRCS(
    yql_yt_lookup_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()

