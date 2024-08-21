INCLUDE_ONCE()

ADDINCL(
    GLOBAL ${ARCADIA_BUILD_ROOT}/${MODDIR}
    GLOBAL ${ARCADIA_BUILD_ROOT}/${MODDIR}/ydb/library/yql/minikql/computation    
)

COPY_FILE(TEXT
    ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h.txt
    ${BINDIR}/ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h
    OUTPUT_INCLUDES
       ${ARCADIA_ROOT}/ydb/library/yql/minikql/computation/mkql_computation_node_codegen_common.h
       llvm/IR/Value.h
       llvm/IR/BasicBlock.h
       llvm/IR/Function.h
       llvm/IR/LLVMContext.h
       llvm/IR/Module.h
       llvm/IR/DerivedTypes.h
       llvm/IR/Instructions.h
       llvm/IR/Constants.h
       llvm/IR/DebugInfoMetadata.h
       llvm/IR/DIBuilder.h
       source_location
)

COPY_FILE(TEXT
    ydb/library/yql/minikql/computation/mkql_llvm_base.h.txt
    ${BINDIR}/ydb/library/yql/minikql/computation/mkql_llvm_base.h
    OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/ydb/library/yql/minikql/computation/mkql_computation_node_impl.h
        llvm/IR/LLVMContext.h
        llvm/IR/Type.h
        llvm/IR/Constant.h
        llvm/IR/DerivedTypes.h
        llvm/IR/Constants.h
)

COPY_FILE(TEXT
    ydb/library/yql/minikql/computation/mkql_block_impl_codegen.h.txt
    ${BINDIR}/ydb/library/yql/minikql/computation/mkql_block_impl_codegen.h
    OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/ydb/library/yql/minikql/computation/mkql_block_item.h
        ${ARCADIA_ROOT}/ydb/library/yql/minikql/codegen/codegen.h
        mkql_llvm_base.h
)
