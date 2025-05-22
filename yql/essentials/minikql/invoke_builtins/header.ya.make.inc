INCLUDE_ONCE()

INCLUDE(${ARCADIA_ROOT}/yql/essentials/minikql/computation/header.ya.make.inc)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/${MODDIR}
    ${ARCADIA_BUILD_ROOT}/${MODDIR}/yql/essentials/minikql/invoke_builtins
    yql/essentials/minikql/invoke_builtins
)

COPY_FILE(TEXT
    yql/essentials/minikql/invoke_builtins/mkql_builtins_codegen.h.txt
    ${BINDIR}/yql/essentials/minikql/invoke_builtins/mkql_builtins_codegen.h
    OUTPUT_INCLUDES
       yql/essentials/minikql/computation/mkql_computation_node_codegen.h
       yql/essentials/minikql/codegen/codegen.h
)

COPY_FILE(TEXT
    yql/essentials/minikql/invoke_builtins/mkql_builtins_codegen_llvm.h.txt
    ${BINDIR}/yql/essentials/minikql/invoke_builtins/mkql_builtins_codegen_llvm.h
    OUTPUT_INCLUDES
        llvm/IR/Constants.h
        llvm/IR/DerivedTypes.h
        llvm/IR/Instructions.h
)

COPY_FILE(TEXT
    yql/essentials/minikql/invoke_builtins/mkql_builtins_impl.h.txt
    ${BINDIR}/yql/essentials/minikql/invoke_builtins/mkql_builtins_impl.h
    OUTPUT_INCLUDES
        mkql_builtins_impl_common.h
        mkql_builtins.h
        mkql_builtins_codegen.h
        mkql_builtins_codegen_llvm.h
        arrow/array/array_base.h
        arrow/array/util.h
)

COPY_FILE(TEXT
    yql/essentials/minikql/invoke_builtins/mkql_builtins_decimal.h.txt
    ${BINDIR}/yql/essentials/minikql/invoke_builtins/mkql_builtins_decimal.h
    OUTPUT_INCLUDES
        mkql_builtins_impl.h
)

