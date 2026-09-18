UNITTEST()

FORK_SUBTESTS()

SRCS(
    purecalc_io_ut.cpp
    scheme_ut.cpp
    uploader_ut.cpp
    writer_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/fq/libs/row_dispatcher/purecalc_no_pg_wrapper
    ydb/core/testlib/basics
    ydb/core/transfer
    ydb/core/tx/replication/service
    ydb/core/tx/scheme_cache
    ydb/core/tx/tx_proxy
    yql/essentials/minikql/codegen/llvm16
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/computation
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/invoke_builtins
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/yson2
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
