UNITTEST_FOR(ydb/library/yql/dq/comp_nodes)

SIZE(MEDIUM)

PEERDIR(
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
    ydb/library/yql/dq/comp_nodes
    yql/essentials/minikql/comp_nodes
    yql/essentials/public/udf
    yql/essentials/core/arrow_kernels/request
    yql/essentials/core/arrow_kernels/registry
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy

    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/codegen/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16

    ydb/library/yql/dq/comp_nodes
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy

    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/codegen/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16

    library/cpp/testing/unittest

    ydb/core/kqp/runtime

    ydb/library/yql/dq/comp_nodes

    contrib/libs/llvm16/lib/IR
    contrib/libs/llvm16/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm16/lib/Linker
    contrib/libs/llvm16/lib/Passes
    contrib/libs/llvm16/lib/Target/X86
    contrib/libs/llvm16/lib/Target/X86/AsmParser
    contrib/libs/llvm16/lib/Target/X86/Disassembler
    contrib/libs/llvm16/lib/Transforms/IPO
    contrib/libs/llvm16/lib/Transforms/ObjCARC
)

YQL_LAST_ABI_VERSION()

SRCS(
    dq_block_hash_join_ut.cpp
)

END() 
