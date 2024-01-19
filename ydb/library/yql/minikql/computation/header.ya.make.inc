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
)
