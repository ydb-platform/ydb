#include "dynamic_function_registry.h"

#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/public/udf/udf_static_registry.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/dynlib.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>

#include <memory>
#include <utility>

namespace NKikimr::NKqp {
namespace {

using namespace NMiniKQL;

const char MODULE_NAME_DELIMITER = '.';
const char* RegisterFuncName = "Register";
const char* AbiVersionFuncName = "AbiVersion";
#if defined(_win_) || defined(_darwin_)
const char* BindSymbolsFuncName = "BindSymbols";
#endif
const char* SetBackTraceCallbackName = "SetBackTraceCallback";

class TDynamicFunctionRegistry: public IDynamicFunctionRegistry {
    struct TUdfModule {
        TString LibraryPath;
        std::shared_ptr<NUdf::IUdfModule> Impl;
    };

    using TUdfModulesMap = THashMap<TString, TUdfModule>;

    struct TUdfLibrary: public TThrRefBase {
        ui32 AbiVersion = 0;
        TDynamicLibrary Lib;
    };
    using TUdfLibraryPtr = TIntrusivePtr<TUdfLibrary>;

    struct TSnapshot: public TAtomicRefCount<TSnapshot> {
        THashMap<TString, TUdfLibraryPtr> LoadedLibraries;
        TUdfModulesMap UdfModules;
        TUdfModulePathsMap SystemModulePaths;
        NUdf::TBackTraceCallback BackTraceCallback = nullptr;
        bool SupportsSizedAllocators = true;

        TSnapshot() = default;

        TSnapshot(const TSnapshot& other)
            : LoadedLibraries(other.LoadedLibraries)
            , UdfModules(other.UdfModules)
            , SystemModulePaths(other.SystemModulePaths)
            , BackTraceCallback(other.BackTraceCallback)
            , SupportsSizedAllocators(other.SupportsSizedAllocators)
        {
        }
    };
    using TSnapshotPtr = TIntrusivePtr<TSnapshot>;

    class TUdfModuleLoader: public NUdf::IRegistrator {
    public:
        TUdfModuleLoader(
            TUdfModulesMap& modulesMap,
            THashSet<TString>* newModules,
            TString libraryPath,
            const TUdfModuleRemappings& remappings,
            ui32 abiVersion,
            TString customUdfPrefix = {})
            : ModulesMap_(modulesMap)
            , NewModules_(newModules)
            , LibraryPath_(std::move(libraryPath))
            , Remappings_(remappings)
            , AbiVersion_(NUdf::AbiVersionToStr(abiVersion))
            , CustomUdfPrefix_(std::move(customUdfPrefix))
        {
        }

        void AddModule(
            const NUdf::TStringRef& name,
            NUdf::TUniquePtr<NUdf::IUdfModule> module) override
        {
            Y_DEBUG_ABORT_UNLESS(module, "Module is empty");

            if (!HasError()) {
                TUdfModule m;
                m.LibraryPath = LibraryPath_;
                m.Impl.reset(module.Release());

                auto it = Remappings_.find(name);
                const TString& newName = CustomUdfPrefix_ + ((it == Remappings_.end())
                                                                 ? TString(name)
                                                                 : it->second);

                auto i = ModulesMap_.insert({newName, std::move(m)});
                if (!i.second) {
                    TUdfModule* oldModule = ModulesMap_.FindPtr(newName);
                    Y_DEBUG_ABORT_UNLESS(oldModule != nullptr);
                    Error_ = (TStringBuilder()
                              << "UDF module duplication: name " << TStringBuf(name)
                              << ", already loaded from " << oldModule->LibraryPath
                              << ", trying to load from " << LibraryPath_);
                } else if (NewModules_) {
                    NewModules_->insert(newName);
                }
            }
        }

        const TString& GetError() const {
            return Error_;
        }
        bool HasError() const {
            return !Error_.empty();
        }

    private:
        TUdfModulesMap& ModulesMap_;
        THashSet<TString>* NewModules_;
        const TString LibraryPath_;
        const TUdfModuleRemappings& Remappings_;
        const TString AbiVersion_;
        TString Error_;
        const TString CustomUdfPrefix_;
    };

public:
    explicit TDynamicFunctionRegistry(IBuiltinFunctionRegistry::TPtr builtins)
        : Builtins_(std::move(builtins))
        , State_(MakeIntrusive<TSnapshot>())
    {
    }

    //! Snapshot is copied; WriterMutex_ / per-path load mutexes are not shared with rhs.
    TDynamicFunctionRegistry(const TDynamicFunctionRegistry& rhs)
        : IDynamicFunctionRegistry()
        , Builtins_(rhs.Builtins_)
        , State_(MakeIntrusive<TSnapshot>(*rhs.State_.AtomicLoad()))
    {
    }

    void AllowUdfPatch() override {
    }

    void LoadUdfs(
        const TString& libraryPath,
        const TUdfModuleRemappings& remmapings,
        ui32 flags = 0,
        const TString& customUdfPrefix = {},
        THashSet<TString>* modules = nullptr) override
    {
        // Native .so only (WASM uses AddModule and never enters here).
        // Per-path mutex: same libraryPath serializes dlopen/Register; different
        // paths load in parallel. Lock order: path load mutex -> WriterMutex_.
        auto pathLoadMutex = AcquireNativePathLoadMutex(libraryPath);
        TGuard<TMutex> loadGuard(*pathLoadMutex);

        TUdfLibraryPtr lib;
        NUdf::TBackTraceCallback backTraceCallback = nullptr;
        bool needOpen = false;
        {
            auto snap = State_.AtomicLoad();
            auto libIt = snap->LoadedLibraries.find(libraryPath);
            if (libIt != snap->LoadedLibraries.end() && libIt->second) {
                lib = libIt->second;
            } else {
                needOpen = true;
                backTraceCallback = snap->BackTraceCallback;
            }
        }

        if (needOpen) {
            auto opened = MakeIntrusive<TUdfLibrary>();
#ifdef _win32_
            ui32 loadFlags = 0;
#else
            ui32 loadFlags = RTLD_GLOBAL | ((flags & NUdf::IRegistrator::TFlags::TypesOnly) ? RTLD_LAZY : RTLD_NOW);
#endif
            TPathSplit absPathSplit(libraryPath);
            TString absPath = libraryPath;
            if (!absPathSplit.IsAbsolute) {
                absPath = JoinPaths(TFsPath::Cwd().PathSplit(), absPathSplit);
            }

            opened->Lib.Open(absPath.data(), loadFlags);
            opened->Lib.SetUnloadable(false);

            auto abiVersionFunc = reinterpret_cast<NUdf::TAbiVersionFunctionPtr>(
                opened->Lib.SymOptional(AbiVersionFuncName));
            if (!abiVersionFunc) {
                return;
            }

            ui32 version = abiVersionFunc();
            Y_ENSURE(NUdf::IsAbiCompatible(version) && version >= NUdf::MakeAbiVersion(2, 8, 0),
                     "Non compatible ABI version of UDF library " << libraryPath
                                                                  << ", expected up to " << NUdf::AbiVersionToStr(NUdf::CurrentCompatibilityAbiVersion() * 100)
                                                                  << ", got " << NUdf::AbiVersionToStr(version)
                                                                  << "; try to re-compile library using "
                                                                  << "YQL_ABI_VERSION(" << UDF_ABI_VERSION_MAJOR
                                                                  << " " << UDF_ABI_VERSION_MINOR << " 0) macro in ya.make");
            opened->AbiVersion = version;

#if defined(_win_) || defined(_darwin_)
            auto bindSymbolsFunc = reinterpret_cast<NUdf::TBindSymbolsFunctionPtr>(
                opened->Lib.Sym(BindSymbolsFuncName));
            bindSymbolsFunc(NUdf::GetStaticSymbols());
#endif

            if (backTraceCallback) {
                auto setter = reinterpret_cast<NUdf::TSetBackTraceCallbackPtr>(
                    opened->Lib.SymOptional(SetBackTraceCallbackName));
                if (setter) {
                    setter(backTraceCallback);
                }
            }

            with_lock (WriterMutex_) {
                auto next = MakeIntrusive<TSnapshot>(*State_.AtomicLoad());
                auto& slot = next->LoadedLibraries[libraryPath];
                if (!slot) {
                    slot = opened;
                }
                lib = slot;
                State_.AtomicStore(next);
            }
        }

        Y_ENSURE(lib, "UDF library handle missing for " << libraryPath);

        auto registerFunc = reinterpret_cast<NUdf::TRegisterFunctionPtr>(
            lib->Lib.Sym(RegisterFuncName));

        TUdfModulesMap staging;
        THashSet<TString> newModules;
        TUdfModuleLoader loader(
            staging,
            &newModules,
            libraryPath,
            remmapings,
            lib->AbiVersion, customUdfPrefix);
        registerFunc(loader, flags);
        Y_ENSURE(!loader.HasError(), loader.GetError());

        with_lock (WriterMutex_) {
            auto next = MakeIntrusive<TSnapshot>(*State_.AtomicLoad());
            for (const auto& [name, module] : staging) {
                Y_UNUSED(module);
                if (const TUdfModule* oldModule = next->UdfModules.FindPtr(name)) {
                    ythrow yexception()
                        << "UDF module duplication: name " << name
                        << ", already loaded from " << oldModule->LibraryPath
                        << ", trying to load from " << libraryPath;
                }
            }
            for (auto& [name, module] : staging) {
                next->UdfModules.emplace(name, std::move(module));
            }
            // Ensure library slot exists even if open raced with a placeholder.
            auto& slot = next->LoadedLibraries[libraryPath];
            if (!slot) {
                slot = lib;
            }
            State_.AtomicStore(next);
        }

        if (modules) {
            *modules = std::move(newModules);
        }
    }

    void AddModule(
        const TStringBuf& libraryPath,
        const TStringBuf& moduleName,
        NUdf::TUniquePtr<NUdf::IUdfModule> module) override
    {
        TString libraryPathStr(libraryPath);
        TUdfModuleRemappings remappings;
        TUdfModulesMap staging;
        TUdfModuleLoader loader(
            staging, /*newModules=*/nullptr, libraryPathStr,
            remappings, NUdf::CurrentAbiVersion());
        loader.AddModule(moduleName, std::move(module));
        Y_ENSURE(!loader.HasError(), loader.GetError());

        with_lock (WriterMutex_) {
            auto next = MakeIntrusive<TSnapshot>(*State_.AtomicLoad());
            // Track path for RemoveModule cleanup; multiple in-memory modules may
            // share one synthetic path (unlike LoadUdfs which opens a real .so once).
            next->LoadedLibraries.emplace(libraryPathStr, nullptr);
            for (const auto& [name, staged] : staging) {
                Y_UNUSED(staged);
                if (const TUdfModule* oldModule = next->UdfModules.FindPtr(name)) {
                    ythrow yexception()
                        << "UDF module duplication: name " << name
                        << ", already loaded from " << oldModule->LibraryPath
                        << ", trying to load from " << libraryPathStr;
                }
            }
            for (auto& [name, staged] : staging) {
                next->UdfModules.emplace(name, std::move(staged));
            }
            State_.AtomicStore(next);
        }
    }

    void RemoveModule(const TStringBuf& moduleName) override {
        TMaybe<TString> droppedLibraryPath;
        with_lock (WriterMutex_) {
            auto cur = State_.AtomicLoad();
            auto it = cur->UdfModules.find(TString(moduleName));
            if (it == cur->UdfModules.end()) {
                return;
            }

            auto next = MakeIntrusive<TSnapshot>(*cur);
            it = next->UdfModules.find(TString(moduleName));
            Y_ABORT_UNLESS(it != next->UdfModules.end());
            const TString libraryPath = it->second.LibraryPath;
            next->UdfModules.erase(it);

            // SystemModulePaths is a separate catalog and must survive unload.
            bool pathStillUsed = false;
            for (const auto& [name, module] : next->UdfModules) {
                Y_UNUSED(name);
                if (module.LibraryPath == libraryPath) {
                    pathStillUsed = true;
                    break;
                }
            }
            if (!pathStillUsed) {
                next->LoadedLibraries.erase(libraryPath);
                droppedLibraryPath = libraryPath;
            }
            State_.AtomicStore(next);
        }
        // Drop per-path load mutex after publish (lock order: never under WriterMutex_).
        if (droppedLibraryPath) {
            ReleaseNativePathLoadMutex(*droppedLibraryPath);
        }
    }

    void SetSystemModulePaths(const TUdfModulePathsMap& paths) override {
        with_lock (WriterMutex_) {
            auto next = MakeIntrusive<TSnapshot>(*State_.AtomicLoad());
            next->SystemModulePaths = paths;
            State_.AtomicStore(next);
        }
    }

    const IBuiltinFunctionRegistry::TPtr& GetBuiltins() const override {
        return Builtins_;
    }

    TStatus FindFunctionTypeInfo(
        NYql::TLangVersion langver,
        const NYql::TRuntimeSettings& runtimeSettings,
        const TTypeEnvironment& env,
        NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
        NUdf::ICountersProvider* countersProvider,
        const TStringBuf& name,
        TType* userType,
        const TStringBuf& typeConfig,
        ui32 flags,
        const NUdf::TSourcePosition& pos,
        const NUdf::ISecureParamsProvider* secureParamsProvider,
        const NUdf::ILogProvider* logProvider,
        TFunctionTypeInfo* funcInfo) const override
    {
        TStringBuf moduleName;
        TStringBuf funcName;
        if (!name.TrySplit(MODULE_NAME_DELIMITER, moduleName, funcName)) {
            return TStatus::Error()
                   << "Function name must be in <module>.<func_name> scheme. "
                   << "But get " << name;
        }

        std::shared_ptr<NUdf::IUdfModule> module;
        {
            auto snap = State_.AtomicLoad();
            auto it = snap->UdfModules.find(moduleName);
            if (it == snap->UdfModules.end()) {
                return TStatus::Error()
                       << "Module " << moduleName << " is not registered";
            }
            module = it->second.Impl;
        }

        TFunctionTypeInfoBuilder typeInfoBuilder(langver, runtimeSettings, env, typeInfoHelper, moduleName,
                                                 (flags & NUdf::IUdfModule::TFlags::TypesOnly) ? nullptr : countersProvider, pos,
                                                 secureParamsProvider, logProvider);
        module->BuildFunctionTypeInfo(
            funcName, userType, typeConfig, flags, typeInfoBuilder);

        if (typeInfoBuilder.HasError()) {
            return TStatus::Error()
                   << "Module: " << moduleName
                   << ", function: " << funcName
                   << ", error: " << typeInfoBuilder.GetError();
        }

        try {
            typeInfoBuilder.Build(funcInfo);
        } catch (yexception& e) {
            return TStatus::Error()
                   << "Module: " << moduleName
                   << ", function: " << funcName
                   << ", error: " << e.what();
        }

        if ((flags & NUdf::IRegistrator::TFlags::TypesOnly) &&
            !funcInfo->FunctionType)
        {
            return TStatus::Error()
                   << "Module: " << moduleName
                   << ", function: " << funcName
                   << ", function not found";
        }

        if (funcInfo->ModuleIRUniqID) {
            funcInfo->ModuleIRUniqID.prepend(moduleName);
        }

        return TStatus::Ok();
    }

    TMaybe<TString> FindUdfPath(const TStringBuf& moduleName) const override {
        auto snap = State_.AtomicLoad();

        if (const TUdfModule* udf = snap->UdfModules.FindPtr(moduleName)) {
            return udf->LibraryPath;
        }

        if (const TString* path = snap->SystemModulePaths.FindPtr(moduleName)) {
            return *path;
        }

        return Nothing();
    }

    bool IsLoadedUdfModule(const TStringBuf& moduleName) const override {
        return State_.AtomicLoad()->UdfModules.contains(moduleName);
    }

    THashSet<TString> GetAllModuleNames() const override {
        auto snap = State_.AtomicLoad();
        THashSet<TString> names;
        names.reserve(snap->UdfModules.size());
        for (const auto& module : snap->UdfModules) {
            names.insert(module.first);
        }
        return names;
    }

    TFunctionsMap GetModuleFunctions(const TStringBuf& moduleName) const override {
        struct TFunctionNamesSink: public NUdf::IFunctionNamesSink {
            TFunctionsMap Functions;
            class TFuncDescriptor: public NUdf::IFunctionDescriptor {
            public:
                explicit TFuncDescriptor(TFunctionProperties& properties)
                    : Properties_(properties)
                {
                }

            private:
                void SetTypeAwareness() final {
                    Properties_.IsTypeAwareness = true;
                }

                void SetPolyArgs(const NUdf::TStringRef& config) final {
                    Properties_.PolyArgs = config;
                }

                TFunctionProperties& Properties_;
            };

            NUdf::IFunctionDescriptor::TPtr Add(const NUdf::TStringRef& name) final {
                const auto it = Functions.emplace(name, TFunctionProperties{});
                return new TFuncDescriptor(it.first->second);
            }
        } sink;

        std::shared_ptr<NUdf::IUdfModule> module;
        {
            auto snap = State_.AtomicLoad();
            const auto it = snap->UdfModules.find(moduleName);
            if (snap->UdfModules.cend() == it) {
                return TFunctionsMap();
            }
            module = it->second.Impl;
        }

        module->GetAllFunctions(sink);
        return sink.Functions;
    }

    bool SupportsSizedAllocators() const override {
        return State_.AtomicLoad()->SupportsSizedAllocators;
    }

    void PrintInfoTo(IOutputStream& out) const override {
        Builtins_->PrintInfoTo(out);
    }

    void CleanupModulesOnTerminate() const override {
        TVector<std::shared_ptr<NUdf::IUdfModule>> modules;
        {
            auto snap = State_.AtomicLoad();
            modules.reserve(snap->UdfModules.size());
            for (const auto& module : snap->UdfModules) {
                modules.push_back(module.second.Impl);
            }
        }
        for (const auto& module : modules) {
            module->CleanupOnTerminate();
        }
    }

    TIntrusivePtr<IMutableFunctionRegistry> Clone() const override {
        return new TDynamicFunctionRegistry(*this);
    }

    void SetBackTraceCallback(NUdf::TBackTraceCallback callback) override {
        with_lock (WriterMutex_) {
            auto next = MakeIntrusive<TSnapshot>(*State_.AtomicLoad());
            next->BackTraceCallback = callback;
            State_.AtomicStore(next);
        }
    }

private:
    //! Per libraryPath mutex for native LoadUdfs (dlopen/Register). WASM uses AddModule.
    std::shared_ptr<TMutex> AcquireNativePathLoadMutex(const TString& libraryPath) {
        with_lock (PathLoadMutexesLock_) {
            auto& slot = PathLoadMutexes_[libraryPath];
            if (!slot) {
                slot = std::make_shared<TMutex>();
            }
            return slot;
        }
    }

    //! Erase only if no in-flight LoadUdfs still holds the shared_ptr; otherwise a
    //! concurrent Acquire would insert a second mutex for the same path and break
    //! per-path serialization.
    void ReleaseNativePathLoadMutex(const TString& libraryPath) {
        with_lock (PathLoadMutexesLock_) {
            auto it = PathLoadMutexes_.find(libraryPath);
            if (it != PathLoadMutexes_.end() && it->second.use_count() == 1) {
                PathLoadMutexes_.erase(it);
            }
        }
    }

    const IBuiltinFunctionRegistry::TPtr Builtins_;

    THotSwap<TSnapshot> State_;
    mutable TAdaptiveLock WriterMutex_;
    TAdaptiveLock PathLoadMutexesLock_;
    THashMap<TString, std::shared_ptr<TMutex>> PathLoadMutexes_;
};

} // namespace

TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> CreateDynamicFunctionRegistry(
    NMiniKQL::IBuiltinFunctionRegistry::TPtr&& builtins)
{
    return new TDynamicFunctionRegistry(std::move(builtins));
}

TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> CreateDynamicFunctionRegistry(
    NKikimr::NUdf::TBackTraceCallback backtraceCallback,
    NMiniKQL::IBuiltinFunctionRegistry::TPtr&& builtins,
    bool allowUdfPatch,
    const TVector<TString>& udfsPaths,
    ui32 flags)
{
    auto registry = MakeHolder<TDynamicFunctionRegistry>(std::move(builtins));
    if (allowUdfPatch) {
        registry->AllowUdfPatch();
    }
    registry->SetBackTraceCallback(backtraceCallback);

    NMiniKQL::TUdfModuleRemappings remappings;
    THashSet<TString> usedUdfPaths;
    for (const TString& udfPath : udfsPaths) {
        if (usedUdfPaths.insert(udfPath).second) {
            registry->LoadUdfs(udfPath, remappings, flags);
        }
    }

    return registry.Release();
}

} // namespace NKikimr::NKqp
