OWNER(
    g:yql
    g:yql_ydb_core
)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/${MODDIR}
    ydb/library/yql/minikql/codegen
)

COPY_FILE(
    AUTO
    ../codegen_llvm_deps.h.txt ${ARCADIA_BUILD_ROOT}/${MODDIR}/codegen_llvm_deps.h
    OUTPUT_INCLUDES
        llvm/ExecutionEngine/ExecutionEngine.h
        llvm/ExecutionEngine/JITEventListener.h
        llvm/ExecutionEngine/MCJIT.h
        llvm/IR/DiagnosticInfo.h
        llvm/IR/DiagnosticPrinter.h
        llvm/IR/LegacyPassManager.h
        llvm/IR/LLVMContext.h
        llvm/IR/Module.h
        llvm/IR/Verifier.h
        llvm/IRReader/IRReader.h
        llvm/Linker/Linker.h
        llvm-c/Disassembler.h
        llvm/Support/Host.h
        llvm/Support/ManagedStatic.h
        llvm/Support/SourceMgr.h
        llvm/Support/TargetSelect.h
        llvm/Support/Timer.h
        llvm/Support/ErrorHandling.h
        llvm/Transforms/IPO.h
        llvm/Transforms/IPO/PassManagerBuilder.h
        llvm/Transforms/Instrumentation.h
        llvm/Transforms/Instrumentation/AddressSanitizer.h
        llvm/Transforms/Instrumentation/MemorySanitizer.h
        llvm/Transforms/Instrumentation/ThreadSanitizer.h
        llvm/LinkAllPasses.h
)


COPY_FILE(
    AUTO
    ../codegen.cpp codegen.cpp
    OUTPUT_INCLUDES
        codegen_llvm_deps.h
)

IF (NOT WINDOWS)
    PEERDIR(
        contrib/libs/cxxsupp/builtins
    )
ELSE()
    PEERDIR(
        ydb/library/yql/public/decimal
    )
ENDIF()

PEERDIR(
    contrib/libs/re2
)

PROVIDES(MINIKQL_CODEGEN)
