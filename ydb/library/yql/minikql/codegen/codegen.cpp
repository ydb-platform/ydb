#include "codegen.h"
Y_PRAGMA_DIAGNOSTIC_PUSH
Y_PRAGMA("GCC diagnostic ignored \"-Wbitwise-instead-of-logical\"")
#include "codegen_llvm_deps.h" // Y_IGNORE
Y_PRAGMA_DIAGNOSTIC_POP
#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/maybe.h>
#include <util/generic/singleton.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/format.h>
#include <util/system/defaults.h>
#include <util/system/platform.h>
#include <util/datetime/base.h>

typedef struct __emutls_control {
    size_t size;  /* size of the object in bytes */
    size_t align;  /* alignment of the object in bytes */
    union {
        uintptr_t index;  /* data[index-1] is the object address */
        void* address;  /* object address, when in single thread env */
    } object;
    void* value;  /* null or non-zero initial value for the object */
} __emutls_control;

#if defined(_msan_enabled_)
extern "C" void* __emutls_get_address(__emutls_control* control);
#endif

class TTlsManager {
public:
    void* Add(const TString& name, size_t size, size_t align) {
        //Cerr << "name: " << name << ", size: " << size << ", align: " << align << "\n";
        auto pair = Tls_.insert(std::make_pair(name, __emutls_control()));
        if (pair.second) {
            Zero(pair.first->second);
            pair.first->second.size = size;
            pair.first->second.align = align;
        }

        return &pair.first->second;
    }

private:
    THashMap<TString, __emutls_control> Tls_;
};

#if !defined(_win_) || defined(__clang__)
extern "C" void __divti3();
extern "C" void __fixdfti();
extern "C" void __fixsfti();
extern "C" void __fixunsdfti();
extern "C" void __floattidf();
extern "C" void __floattisf();
extern "C" void __floatuntidf();
extern "C" void __floatuntisf();
extern "C" void __modti3();
extern "C" void __muloti4();
extern "C" void __udivti3();
extern "C" void __umodti3();
#else
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#define CRT_HAS_128BIT
#define INT_LIB_H
#define COMPILER_RT_ABI
typedef NYql::NDecimal::TInt128 ti_int;
typedef NYql::NDecimal::TUint128 tu_int;

typedef      int si_int;
typedef unsigned su_int;

typedef          long long di_int;
typedef unsigned long long du_int;

typedef union
{
    tu_int all;
    struct
    {
        du_int low;
        du_int high;
    }s;
} utwords;

typedef union
{
    ti_int all;
    struct
    {
        du_int low;
        di_int high;
    }s;
} twords;

typedef union
{
    du_int all;
    struct
    {
        su_int low;
        su_int high;
    }s;
} udwords;

typedef union
{
    su_int u;
    float f;
} float_bits;

typedef union
{
    udwords u;
    double  f;
} double_bits;

int __builtin_ctzll(ui64 value) {
    DWORD trailing_zero = 0;
    if (_BitScanForward64(&trailing_zero, value)) {
        return trailing_zero;
    } else {
        return 64;
    }
}

int __builtin_clzll(ui64 value) {
    DWORD leading_zero = 0;
    if (_BitScanReverse64(&leading_zero, value)) {
        return 63 - leading_zero;
    } else {
        return 64;
    }
}

#define __divti3 __divti3impl
#define __udivmodti4 __udivmodti4impl
#define __modti3 __modti3impl
#define __clzti2 __clzti2impl
#define __floattisf __floattisfimpl
#define __floattidf __floattidfimpl

#include <contrib/libs/cxxsupp/builtins/udivmodti4.c>
#include <contrib/libs/cxxsupp/builtins/divti3.c>
#include <contrib/libs/cxxsupp/builtins/modti3.c>
#include <contrib/libs/cxxsupp/builtins/clzti2.c>
#include <contrib/libs/cxxsupp/builtins/floattisf.c>
#include <contrib/libs/cxxsupp/builtins/floattidf.c>
#include <intrin.h>
#include <xmmintrin.h>

// Return value in XMM0
__m128 __vectorcall __divti3abi(ti_int* x, ti_int* y) {
    __m128 ret;
    auto z = __divti3(*x, *y);
    memcpy(&ret, &z, sizeof(ti_int));
    return ret;
}

// Return value in XMM0
__m128 __vectorcall __modti3abi(ti_int* x, ti_int* y) {
    __m128 ret;
    auto z = __modti3(*x, *y);
    memcpy(&ret, &z, sizeof(ti_int));
    return ret;
}

float __floattisfabi(du_int x, du_int y) {
    utwords t;
    t.s.low = x;
    t.s.high = y;
    return __floattisf(t.all);
}

double __floattidfabi(du_int x, du_int y) {
    utwords t;
    t.s.low = x;
    t.s.high = y;
    return __floattidf(t.all);
}

#endif

namespace NYql {
namespace NCodegen {

namespace {

    void FatalErrorHandler(void* user_data,
#if LLVM_VERSION_MAJOR == 12
    const std::string& reason
#else
    const char* reason
#endif
    , bool gen_crash_diag) {
        Y_UNUSED(user_data);
        Y_UNUSED(gen_crash_diag);
        ythrow yexception() << "LLVM fatal error: " << reason;
    }

    void AddAddressSanitizerPasses(const llvm::PassManagerBuilder& builder, llvm::legacy::PassManagerBase& pm) {
        Y_UNUSED(builder);
        pm.add(llvm::createAddressSanitizerFunctionPass());
        pm.add(llvm::createModuleAddressSanitizerLegacyPassPass());
    }

    void AddMemorySanitizerPass(const llvm::PassManagerBuilder& builder, llvm::legacy::PassManagerBase& pm) {
        Y_UNUSED(builder);
        pm.add(llvm::createMemorySanitizerLegacyPassPass());
    }

    void AddThreadSanitizerPass(const llvm::PassManagerBuilder& builder, llvm::legacy::PassManagerBase& pm) {
        Y_UNUSED(builder);
        pm.add(llvm::createThreadSanitizerLegacyPassPass());
    }

    struct TCodegenInit {
        TCodegenInit() {
            llvm::InitializeNativeTarget();
            llvm::InitializeNativeTargetAsmPrinter();
            llvm::InitializeNativeTargetAsmParser();
            llvm::InitializeNativeTargetDisassembler();
            llvm::install_fatal_error_handler(&FatalErrorHandler, nullptr);
        }
    };

    bool CompareFuncOffsets(const std::pair<ui64, llvm::Function*>& lhs,
        const std::pair<ui64, llvm::Function*>& rhs) {
        return lhs.first < rhs.first;
    }
}

bool ICodegen::IsCodegenAvailable() {
    return true;
}

class TCodegen : public ICodegen, private llvm::JITEventListener {
public:
    TCodegen(ETarget target, ESanitize sanitize)
        : Target_(target), Sanitize_(sanitize)
        , EffectiveTarget_(Target_), EffectiveSanitize_(Sanitize_)
    {
        Singleton<TCodegenInit>();
        Context_.setDiagnosticHandlerCallBack(&DiagnosticHandler, this);

        std::unique_ptr<llvm::Module> module(new llvm::Module("yql", Context_));
        Module_ = module.get();
        std::string triple;
        if (EffectiveTarget_ == ETarget::Native && EffectiveSanitize_ == ESanitize::Auto) {
#if defined(_asan_enabled_)
            EffectiveSanitize_ = ESanitize::Asan;
#elif defined(_tsan_enabled_)
            EffectiveSanitize_ = ESanitize::Tsan;
#elif defined(_msan_enabled_)
            EffectiveSanitize_ = ESanitize::Msan;
#endif
        }

        if (EffectiveTarget_ == ETarget::CurrentOS || EffectiveTarget_ == ETarget::Native) {
#if defined(_linux_)
            EffectiveTarget_ = ETarget::Linux;
#elif defined(_darwin_)
            EffectiveTarget_ = ETarget::Darwin;
#elif defined(_win_)
            EffectiveTarget_ = ETarget::Windows;
#else
#error Unsupported OS
#endif
        }


        switch (EffectiveTarget_) {
        case ETarget::Linux:
            triple = "x86_64-unknown-linux-gnu";
            break;
        case ETarget::Darwin:
            triple = "x86_64-apple-darwin";
            break;
        case ETarget::Windows:
            triple = "x86_64-unknown-windows-msvc";
            break;
        default:
            ythrow yexception() << "Failed to select target";
        }

        Triple_ = llvm::Triple::normalize(triple);
        Module_->setTargetTriple(Triple_);
        Module_->addModuleFlag(llvm::Module::Warning, "Dwarf Version", llvm::dwarf::DWARF_VERSION);
        Module_->addModuleFlag(llvm::Module::Warning, "Debug Info Version", llvm::DEBUG_METADATA_VERSION);

        llvm::TargetOptions targetOptions;
        targetOptions.EnableFastISel = true;
        // init manually, this field was lost in llvm::TargetOptions ctor :/
        // make coverity happy
#if LLVM_VERSION_MAJOR == 12
        targetOptions.StackProtectorGuardOffset = 0;
#endif        

        std::string what;
        auto&& engineBuilder = llvm::EngineBuilder(std::move(module));
        engineBuilder
            .setEngineKind(llvm::EngineKind::JIT)
            .setOptLevel(llvm::CodeGenOpt::Default)
            .setErrorStr(&what)
            .setTargetOptions(targetOptions);

        if (Target_ == ETarget::Native) {
            auto hostCpu = llvm::sys::getHostCPUName();
            engineBuilder.setMCPU(hostCpu);
        }

        Engine_.reset(engineBuilder.create());
        if (!Engine_)
            ythrow yexception() << "Failed to construct ExecutionEngine: " << what;

        Module_->setDataLayout(Engine_->getDataLayout().getStringRepresentation());
        Engine_->RegisterJITEventListener(this);
    }

    void TogglePerfJITEventListener() override {
#ifdef __linux__
        PerfListener_ = llvm::JITEventListener::createPerfJITEventListener();
        Engine_->RegisterJITEventListener(PerfListener_);
#endif
    }

    ~TCodegen() {
#ifdef __linux__
        if (PerfListener_) {
            Engine_->UnregisterJITEventListener(PerfListener_);
        }
#endif
        Engine_->UnregisterJITEventListener(this);
    }

    ETarget GetEffectiveTarget() const override {
        return EffectiveTarget_;
    }

    llvm::LLVMContext& GetContext() override {
        return Context_;
    }


    llvm::Module& GetModule() override {
        return *Module_;
    }

    llvm::ExecutionEngine& GetEngine() override {
        return *Engine_;
    }

    void Verify() override {
        std::string what;
        llvm::raw_string_ostream os(what);
        if (llvm::verifyModule(*Module_, &os)) {
            ythrow yexception() << "Verification error: " << what;
        }
    }

    void GetStats(TCodegenStats& stats) override {
        TCodegenStats ret;
        for (auto& func : Module_->functions()) {
            if (func.isDeclaration()) {
                continue;
            }

            ui64 instructions = func.getInstructionCount();
            ret.TotalInstructions += instructions;
            ret.MaxFunctionInstructions = Max(ret.MaxFunctionInstructions, instructions);
            ++ret.TotalFunctions;
        }

        stats = ret;
    }

    void ExportSymbol(llvm::Function* function) override {
        if (!ExportedSymbols) {
            ExportedSymbols.ConstructInPlace();
        }

        auto name = function->getName();
        ExportedSymbols->emplace(TString(name.data(), name.size()));
    }

    void Compile(const TStringBuf compileOpts, TCompileStats* compileStats) override {

        bool dumpTimers = compileOpts.Contains("time-passes");
        bool disableOpt = compileOpts.Contains("disable-opt");
#ifndef NDEBUG
        disableOpt = true;
#endif

#if defined(_msan_enabled_)
        ReverseGlobalMapping_[(const void*)&__emutls_get_address] = "__emutls_get_address";
#endif
#if defined(_win_) && !defined(__clang__)
        AddGlobalMapping("__security_check_cookie", (const void*)&__security_check_cookie);
        AddGlobalMapping("__security_cookie", (const void*)&__security_cookie);
        AddGlobalMapping("__divti3", (const void*)&__divti3abi);
        AddGlobalMapping("__modti3", (const void*)&__modti3abi);
        AddGlobalMapping("__floattisf", (const void*)&__floattisfabi);
        AddGlobalMapping("__floattidf", (const void*)&__floattidfabi);
#else
        AddGlobalMapping("__divti3", (const void*)&__divti3);
        AddGlobalMapping("__fixdfti", (const void*)&__fixdfti);
        AddGlobalMapping("__fixsfti", (const void*)&__fixsfti);
        AddGlobalMapping("__fixunsdfti", (const void*)&__fixunsdfti);
        AddGlobalMapping("__floattidf", (const void*)&__floattidf);
        AddGlobalMapping("__floattisf", (const void*)&__floattisf);
        AddGlobalMapping("__floatuntidf", (const void*)&__floatuntidf);
        AddGlobalMapping("__floatuntisf", (const void*)&__floatuntisf);
        AddGlobalMapping("__modti3", (const void*)&__modti3);
        AddGlobalMapping("__muloti4", (const void*)&__muloti4);
        AddGlobalMapping("__udivti3", (const void*)&__udivti3);
        AddGlobalMapping("__umodti3", (const void*)&__umodti3);
#endif

        for (auto& function : Module_->getFunctionList()) {
            function.addFnAttr("target-cpu", "x86-64");
            function.addFnAttr("target-features", "+sse,+sse2");
        }

        if (dumpTimers) {
            llvm::TimePassesIsEnabled = true;
        }

        std::unique_ptr<llvm::legacy::PassManager> modulePassManager;
        std::unique_ptr<llvm::legacy::FunctionPassManager> functionPassManager;

        if (ExportedSymbols) {
            modulePassManager = std::make_unique<llvm::legacy::PassManager>();
            modulePassManager->add(llvm::createInternalizePass([&](const llvm::GlobalValue& gv) -> bool {
                auto name = TString(gv.getName().str());
                return ExportedSymbols->contains(name);
            }));

            modulePassManager->add(llvm::createGlobalDCEPass());
            modulePassManager->run(*Module_);
        }

        llvm::PassManagerBuilder passManagerBuilder;
        passManagerBuilder.OptLevel = disableOpt ? 0 : 2;
        passManagerBuilder.SizeLevel = 0;
        passManagerBuilder.Inliner = llvm::createFunctionInliningPass();

        if (EffectiveSanitize_ == ESanitize::Asan) {
            passManagerBuilder.addExtension(llvm::PassManagerBuilder::EP_OptimizerLast,
                           AddAddressSanitizerPasses);
            passManagerBuilder.addExtension(llvm::PassManagerBuilder::EP_EnabledOnOptLevel0,
                           AddAddressSanitizerPasses);
        }

        if (EffectiveSanitize_ == ESanitize::Msan) {
            passManagerBuilder.addExtension(llvm::PassManagerBuilder::EP_OptimizerLast,
                           AddMemorySanitizerPass);
            passManagerBuilder.addExtension(llvm::PassManagerBuilder::EP_EnabledOnOptLevel0,
                           AddMemorySanitizerPass);
        }

        if (EffectiveSanitize_ == ESanitize::Tsan) {
            passManagerBuilder.addExtension(llvm::PassManagerBuilder::EP_OptimizerLast,
                           AddThreadSanitizerPass);
            passManagerBuilder.addExtension(llvm::PassManagerBuilder::EP_EnabledOnOptLevel0,
                           AddThreadSanitizerPass);
        }

        functionPassManager = std::make_unique<llvm::legacy::FunctionPassManager>(Module_);
        modulePassManager = std::make_unique<llvm::legacy::PassManager>();

        passManagerBuilder.populateModulePassManager(*modulePassManager);
        passManagerBuilder.populateFunctionPassManager(*functionPassManager);

        auto functionPassStart = Now();
        functionPassManager->doInitialization();
        for (auto it = Module_->begin(), jt = Module_->end(); it != jt; ++it) {
            if (!it->isDeclaration()) {
                functionPassManager->run(*it);
            }
        }
        functionPassManager->doFinalization();

        if (compileStats) {
            compileStats->FunctionPassTime = (Now() - functionPassStart).MilliSeconds();
        }

        auto modulePassStart = Now();
        modulePassManager->run(*Module_);
        if (compileStats) {
            compileStats->ModulePassTime = (Now() - modulePassStart).MilliSeconds();
        }

        AllocateTls();

        auto finalizeStart = Now();
        Engine_->finalizeObject();
        if (compileStats) {
            compileStats->FinalizeTime = (Now() - finalizeStart).MilliSeconds();
        }

        for (const auto& secEntry : CodeSections_) {
            for (auto& func : Module_->functions()) {
                if (func.isDeclaration()) {
                    continue;
                }

                auto addr = (ui64)Engine_->getPointerToFunction(&func);
                if (addr < secEntry.second || addr >= secEntry.second + secEntry.first.getSize()) {
                    continue;
                }

                SortedFuncs_.emplace_back(addr, &func);
            }

            SortedFuncs_.emplace_back(secEntry.second + secEntry.first.getSize(), nullptr);
        }

        std::sort(SortedFuncs_.begin(), SortedFuncs_.end(), CompareFuncOffsets);
        if (dumpTimers) {
            llvm::TimerGroup::printAll(llvm::errs());
            llvm::TimePassesIsEnabled = false;
        }

        if (compileStats) {
            compileStats->TotalObjectSize = TotalObjectSize;
        }
    }

    void* GetPointerToFunction(llvm::Function* function) override {
        return Engine_->getPointerToFunction(function);
    }

    ui64 GetFunctionCodeSize(llvm::Function* function) override {
        auto addr = (ui64)Engine_->getPointerToFunction(function);
        auto it = std::upper_bound(SortedFuncs_.begin(), SortedFuncs_.end(), std::make_pair(addr, nullptr), CompareFuncOffsets);
        return it->first - addr;
    }

    void ShowGeneratedFunctions(IOutputStream* out) override {
        *out << "--- functions begin ---\n";
        for (const auto& secEntry : CodeSections_) {
            auto expName = secEntry.first.getName();
            auto name = expName.get();
            auto sectionName = TStringBuf(name.data(), name.size());
            *out << "section: " << sectionName << ", addr: " << (void*)secEntry.second << ", size: " << secEntry.first.getSize() << "\n";
        }

        for (const auto& funcEntry : SortedFuncs_) {
            if (!funcEntry.second) {
                continue;
            }

            const auto& name = funcEntry.second->getName();
            auto funcName = TStringBuf(name.data(), name.size());
            auto codeSize = GetFunctionCodeSize(funcEntry.second);
            *out << "function: " << funcName << ", addr: " << (void*)funcEntry.first << ", size: " <<
                codeSize << "\n";

            Disassemble(out, (const unsigned char*)funcEntry.first, codeSize);
        }

        *out << "--- functions end ---\n";
    }

    void Disassemble(IOutputStream* out, const unsigned char* buf, size_t size) {
        InitRegexps();
        auto dis = LLVMCreateDisasm(Triple_.c_str(), nullptr, 0, nullptr, nullptr);
        if (!dis) {
            ythrow yexception() << "Cannot create disassembler";
        }

        std::unique_ptr<void, void(*)(void*)> delDis(dis, LLVMDisasmDispose);
        LLVMSetDisasmOptions(dis, LLVMDisassembler_Option_AsmPrinterVariant);
        char outline[1024];
        size_t pos = 0;
        while (pos < size) {
            size_t l = LLVMDisasmInstruction(dis, (uint8_t*)buf + pos, size - pos, 0, outline, sizeof(outline));
            if (!l) {
                *out << "  " << LeftPad(pos, 4, '0') << "\t???";
                ++pos;
            } else {
                *out << "  " << LeftPad(pos, 4, '0') << outline;
                TStringBuf s(outline);
                const re2::StringPiece piece(s.data(), s.size());
                std::array<re2::StringPiece, 2> captures;
                if (Patterns_->Imm_.Match(piece, 0, s.size(), re2::RE2::UNANCHORED, captures.data(), captures.size())) {
                    auto numBuf = TStringBuf(captures[1].data(), captures[1].size());
                    ui64 addr = FromString<ui64>(numBuf);
                    auto it = ReverseGlobalMapping_.find((void*)addr);
                    if (it != ReverseGlobalMapping_.end()) {
                        *out << " ; &" << it->second;
                    }
                } else if (Patterns_->Jump_.Match(piece, 0, s.size(), re2::RE2::UNANCHORED, captures.data(), captures.size())) {
                    auto numBuf = TStringBuf(captures[1].data(), captures[1].size());
                    i64 offset = FromString<i64>(numBuf);
                    *out << " ; -> " << pos + l + offset;
                }

                pos += l;
            }

            *out << '\n';
        }
    }

    void LoadBitCode(TStringBuf bitcode, TStringBuf uniqId) override {
        if (uniqId && LoadedModules_.contains(uniqId)) {
            return;
        }
        llvm::SMDiagnostic error;
        auto buffer = llvm::MemoryBuffer::getMemBuffer(
            llvm::StringRef(bitcode.data(), bitcode.size()));
        std::unique_ptr<llvm::Module> module = llvm::parseIR(buffer->getMemBufferRef(), error, Context_);

        if (!module) {
            std::string what;
            llvm::raw_string_ostream os(what);
            error.print("error after ParseIR()", os);
            ythrow yexception() << what;
        }

        module->setTargetTriple(Triple_);
        module->setDataLayout(Engine_->getDataLayout().getStringRepresentation());
        if (uniqId) {
            module->setModuleIdentifier(llvm::StringRef(uniqId.data(), uniqId.size()));
        }

        if (llvm::Linker::linkModules(*Module_, std::move(module))) {
            TString err;
            err.append("LLVM: error linking module");
            if (uniqId) {
                err.append(' ').append(uniqId);
            }
            if (Diagnostic_.size()) {
                err.append(": ").append(Diagnostic_.c_str(), Diagnostic_.size());
            }
            ythrow yexception() << err;
        }

        if (uniqId) {
            LoadedModules_.emplace(uniqId);
        }
    }

    void AddGlobalMapping(TStringBuf name, const void* address) override {
        ReverseGlobalMapping_[address] = TString(name);
        Engine_->updateGlobalMapping(llvm::StringRef(name.data(), name.size()), (uint64_t)address);
    }

    void notifyObjectLoaded(ObjectKey key, const llvm::object::ObjectFile &obj,
                            const llvm::RuntimeDyld::LoadedObjectInfo &loi) override
    {
        Y_UNUSED(key);
        TotalObjectSize += obj.getData().size();

        for (const auto& section : obj.sections()) {
            //auto nameExp = section.getName();
            //auto name = nameExp.get();
            //auto nameStr = TStringBuf(name.data(), name.size());
            //Cerr << nameStr << "\n";
            if (section.isText()) {
                CodeSections_.emplace_back(section, loi.getSectionLoadAddress(section));
            }
        }
    }

private:
    void OnDiagnosticInfo(const llvm::DiagnosticInfo &info) {
        llvm::raw_string_ostream ostream(Diagnostic_);
        llvm::DiagnosticPrinterRawOStream printer(ostream);
        info.print(printer);
    }

    static void DiagnosticHandler(const llvm::DiagnosticInfo &info, void* context) {
        return static_cast<TCodegen*>(context)->OnDiagnosticInfo(info);
    }

    void AllocateTls() {
        for (const auto& glob : Module_->globals()) {
            auto nameRef = glob.getName();
            if (glob.isThreadLocal()) {
                llvm::Type* type = glob.getValueType();
                const llvm::DataLayout& dataLayout = Module_->getDataLayout();
                auto size = dataLayout.getTypeStoreSize(type);
                auto align = glob.getAlignment();
                if (!align) {
                    // When LLVM IL declares a variable without alignment, use
                    // the ABI default alignment for the type.
                    align = dataLayout.getABITypeAlignment(type);
                }

                TStringBuf name(nameRef.data(), nameRef.size());
                TString fullName = TString("__emutls_v.") + name;
                auto ctl = TlsManager_.Add(fullName, size, align);
                Engine_->updateGlobalMapping(llvm::StringRef(fullName.data(), fullName.size()), (uint64_t)ctl);
                ReverseGlobalMapping_[&ctl] = fullName;
            }
        }
    }

    struct TPatterns {
        TPatterns()
            : Imm_(re2::StringPiece("\\s*movabs\\s+[0-9a-z]+\\s*,\\s*(\\d+)\\s*"))
            , Jump_(re2::StringPiece("\\s*(?:j[a-z]+)\\s*(-?\\d+)\\s*"))
        {}

        re2::RE2 Imm_;
        re2::RE2 Jump_;
    };

    void InitRegexps() {
        if (!Patterns_) {
            Patterns_.ConstructInPlace();
        }
    }

    const ETarget Target_;
    const ESanitize Sanitize_;
    ETarget EffectiveTarget_;
    ESanitize EffectiveSanitize_;
    llvm::LLVMContext Context_;
    std::string Diagnostic_;
    std::string Triple_;
    llvm::Module* Module_;
#ifdef __linux__    
    llvm::JITEventListener* PerfListener_ = nullptr;
#endif
    std::unique_ptr<llvm::ExecutionEngine> Engine_;
    std::vector<std::pair<llvm::object::SectionRef, ui64>> CodeSections_;
    ui64 TotalObjectSize = 0;
    std::vector<std::pair<ui64, llvm::Function*>> SortedFuncs_;
    TMaybe<THashSet<TString>> ExportedSymbols;
    THashMap<const void*, TString> ReverseGlobalMapping_;
    TMaybe<TPatterns> Patterns_;
    TTlsManager TlsManager_;
    THashSet<TString> LoadedModules_;
};

ICodegen::TPtr
ICodegen::Make(ETarget target, ESanitize sanitize) {
    return std::make_unique<TCodegen>(target, sanitize);
}

ICodegen::TSharedPtr
ICodegen::MakeShared(ETarget target, ESanitize sanitize) {
    return std::make_shared<TCodegen>(target, sanitize);
}

}
}
