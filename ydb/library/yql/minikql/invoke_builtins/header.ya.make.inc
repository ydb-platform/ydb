INCLUDE_ONCE()

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/minikql/computation/header.ya.make.inc)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/${MODDIR}
    ${ARCADIA_BUILD_ROOT}/${MODDIR}/ydb/library/yql/minikql/invoke_builtins
    ydb/library/yql/minikql/invoke_builtins
)

COPY_FILE(TEXT
    ydb/library/yql/minikql/invoke_builtins/mkql_builtins_codegen.h.txt
    ${BINDIR}/ydb/library/yql/minikql/invoke_builtins/mkql_builtins_codegen.h
    OUTPUT_INCLUDES
       ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h
       ydb/library/yql/minikql/codegen/codegen.h
)

COPY_FILE(TEXT
    ydb/library/yql/minikql/invoke_builtins/mkql_builtins_codegen_llvm.h.txt
    ${BINDIR}/ydb/library/yql/minikql/invoke_builtins/mkql_builtins_codegen_llvm.h
    OUTPUT_INCLUDES
        llvm/IR/Constants.h
        llvm/IR/DerivedTypes.h
        llvm/IR/Instructions.h
)

COPY_FILE(TEXT
    ydb/library/yql/minikql/invoke_builtins/mkql_builtins_impl.h.txt
    ${BINDIR}/ydb/library/yql/minikql/invoke_builtins/mkql_builtins_impl.h
    OUTPUT_INCLUDES
        mkql_builtins_impl_common.h
        mkql_builtins.h
        mkql_builtins_codegen.h
        mkql_builtins_codegen_llvm.h
        arrow/array/array_base.h
        arrow/array/util.h
)

COPY_FILE(TEXT
    ydb/library/yql/minikql/invoke_builtins/mkql_builtins_decimal.h.txt
    ${BINDIR}/ydb/library/yql/minikql/invoke_builtins/mkql_builtins_decimal.h
    OUTPUT_INCLUDES
        mkql_builtins_impl.h
)

