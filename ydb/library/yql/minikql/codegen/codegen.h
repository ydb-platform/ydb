#pragma once

#include <memory>
#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>

class IOutputStream;

namespace llvm {
    class Module;
    class Function;
    class LLVMContext;
    class ExecutionEngine;
}

namespace NYql {
namespace NCodegen {

enum class ETarget {
    Native, // Run on current processor and OS
    CurrentOS,
    Linux,
    Windows,
    Darwin
};

enum class ESanitize {
    Auto,
    Asan,
    Msan,
    Tsan
};

struct TCodegenStats {
    ui64 TotalFunctions = 0;
    ui64 TotalInstructions = 0;
    ui64 MaxFunctionInstructions = 0;
};

struct TCompileStats {
    ui32 FunctionPassTime = 0;
    ui32 ModulePassTime = 0;
    ui32 FinalizeTime = 0;
    ui64 TotalObjectSize = 0;
};

class ICodegen {
public:
    virtual ~ICodegen() = default;
    virtual ETarget GetEffectiveTarget() const = 0;
    virtual llvm::LLVMContext& GetContext() = 0;
    virtual llvm::Module& GetModule() = 0;
    virtual llvm::ExecutionEngine& GetEngine() = 0;
    virtual void Verify() = 0;
    virtual void GetStats(TCodegenStats& stats) = 0;
    virtual void ExportSymbol(llvm::Function* function) = 0; // to run DCE before Compile
    virtual void Compile(const TStringBuf compileOpts = TStringBuf(), TCompileStats* compileStats = nullptr) = 0;
    virtual void* GetPointerToFunction(llvm::Function* function) = 0;
    virtual ui64 GetFunctionCodeSize(llvm::Function* function) = 0;
    virtual void ShowGeneratedFunctions(IOutputStream* out) = 0;
    virtual void LoadBitCode(TStringBuf bitcode, TStringBuf uniqId) = 0;
    virtual void AddGlobalMapping(TStringBuf name, const void* address) = 0;
    virtual void TogglePerfJITEventListener() = 0;

    using TPtr = std::unique_ptr<ICodegen>;
    static TPtr Make(ETarget target, ESanitize sanitize = ESanitize::Auto);

    using TSharedPtr = std::shared_ptr<ICodegen>;
    static TSharedPtr MakeShared(ETarget target, ESanitize sanitize = ESanitize::Auto);

    static bool IsCodegenAvailable();
};

}
}
