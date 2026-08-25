#include "dynamic_function_registry.h"

#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/public/udf/udf_static_registry.h>

#include <util/folder/path.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/dynlib.h>
#include <util/system/rwlock.h>

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
    {
    }

    //! Caller must hold rhs.Lock_ (shared or exclusive), or rhs must be unused by
    //! other threads. Lock_ itself is not copied (default-constructed).
    TDynamicFunctionRegistry(const TDynamicFunctionRegistry& rhs)
        : IDynamicFunctionRegistry()
        , Builtins_(rhs.Builtins_)
        , LoadedLibraries_(rhs.LoadedLibraries_)
        , UdfModules_(rhs.UdfModules_)
        , SystemModulePaths_(rhs.SystemModulePaths_)
        , BackTraceCallback_(rhs.BackTraceCallback_)
        , SupportsSizedAllocators_(rhs.SupportsSizedAllocators_)
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
        TWriteGuard guard(Lock_);

        TUdfLibraryPtr lib;

        auto libIt = LoadedLibraries_.find(libraryPath);
        if (libIt == LoadedLibraries_.end()) {
            lib = MakeIntrusive<TUdfLibrary>();
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

            lib->Lib.Open(absPath.data(), loadFlags);
            lib->Lib.SetUnloadable(false);

            auto abiVersionFunc = reinterpret_cast<NUdf::TAbiVersionFunctionPtr>(
                lib->Lib.SymOptional(AbiVersionFuncName));
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
            lib->AbiVersion = version;
            if (version < NUdf::MakeAbiVersion(2, 8, 0)) {
                SupportsSizedAllocators_ = false;
            }

#if defined(_win_) || defined(_darwin_)
            auto bindSymbolsFunc = reinterpret_cast<NUdf::TBindSymbolsFunctionPtr>(lib->Lib.Sym(BindSymbolsFuncName));
            bindSymbolsFunc(NUdf::GetStaticSymbols());
#endif

            if (BackTraceCallback_) {
                auto setter = reinterpret_cast<NUdf::TSetBackTraceCallbackPtr>(lib->Lib.SymOptional(SetBackTraceCallbackName));
                if (setter) {
                    setter(BackTraceCallback_);
                }
            }

            libIt = LoadedLibraries_.insert({libraryPath, lib}).first;
        } else {
            lib = libIt->second;
        }

        auto registerFunc = reinterpret_cast<NUdf::TRegisterFunctionPtr>(
            lib->Lib.Sym(RegisterFuncName));

        THashSet<TString> newModules;
        TUdfModuleLoader loader(
            UdfModules_,
            &newModules,
            libraryPath,
            remmapings,
            lib->AbiVersion, customUdfPrefix);
        registerFunc(loader, flags);
        Y_ENSURE(!loader.HasError(), loader.GetError());

        if (modules) {
            *modules = std::move(newModules);
        }
    }

    void AddModule(
        const TStringBuf& libraryPath,
        const TStringBuf& moduleName,
        NUdf::TUniquePtr<NUdf::IUdfModule> module) override
    {
        TWriteGuard guard(Lock_);

        TString libraryPathStr(libraryPath);
        // Track the path for RemoveModule cleanup; multiple in-memory modules may
        // share one synthetic path (unlike LoadUdfs which opens a real .so once).
        LoadedLibraries_.emplace(libraryPathStr, nullptr);

        TUdfModuleRemappings remappings;
        TUdfModuleLoader loader(
            UdfModules_, /*newModules=*/nullptr, libraryPathStr,
            remappings, NUdf::CurrentAbiVersion());
        loader.AddModule(moduleName, std::move(module));

        Y_ENSURE(!loader.HasError(), loader.GetError());
    }

    void RemoveModule(const TStringBuf& moduleName) override {
        TWriteGuard guard(Lock_);

        // Only UdfModules_ / LoadedLibraries_. SystemModulePaths_ is a separate
        // catalog and must survive unload so FindUdfPath can still resolve it.
        auto it = UdfModules_.find(TString(moduleName));
        if (it == UdfModules_.end()) {
            return;
        }
        const TString libraryPath = it->second.LibraryPath;
        UdfModules_.erase(it);
        bool pathStillUsed = false;
        for (const auto& [name, module] : UdfModules_) {
            Y_UNUSED(name);
            if (module.LibraryPath == libraryPath) {
                pathStillUsed = true;
                break;
            }
        }
        if (!pathStillUsed) {
            LoadedLibraries_.erase(libraryPath);
        }
    }

    void SetSystemModulePaths(const TUdfModulePathsMap& paths) override {
        TWriteGuard guard(Lock_);
        SystemModulePaths_ = paths;
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
            TReadGuard guard(Lock_);
            auto it = UdfModules_.find(moduleName);
            if (it == UdfModules_.end()) {
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
        TReadGuard guard(Lock_);

        if (const TUdfModule* udf = UdfModules_.FindPtr(moduleName)) {
            return udf->LibraryPath;
        }

        if (const TString* path = SystemModulePaths_.FindPtr(moduleName)) {
            return *path;
        }

        return Nothing();
    }

    bool IsLoadedUdfModule(const TStringBuf& moduleName) const override {
        TReadGuard guard(Lock_);
        return UdfModules_.contains(moduleName);
    }

    THashSet<TString> GetAllModuleNames() const override {
        TReadGuard guard(Lock_);

        THashSet<TString> names;
        names.reserve(UdfModules_.size());
        for (const auto& module : UdfModules_) {
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
            TReadGuard guard(Lock_);
            const auto it = UdfModules_.find(moduleName);
            if (UdfModules_.cend() == it) {
                return TFunctionsMap();
            }
            module = it->second.Impl;
        }

        module->GetAllFunctions(sink);
        return sink.Functions;
    }

    bool SupportsSizedAllocators() const override {
        TReadGuard guard(Lock_);
        return SupportsSizedAllocators_;
    }

    void PrintInfoTo(IOutputStream& out) const override {
        Builtins_->PrintInfoTo(out);
    }

    void CleanupModulesOnTerminate() const override {
        TVector<std::shared_ptr<NUdf::IUdfModule>> modules;
        {
            TReadGuard guard(Lock_);
            modules.reserve(UdfModules_.size());
            for (const auto& module : UdfModules_) {
                modules.push_back(module.second.Impl);
            }
        }
        for (const auto& module : modules) {
            module->CleanupOnTerminate();
        }
    }

    TIntrusivePtr<IMutableFunctionRegistry> Clone() const override {
        TReadGuard guard(Lock_);
        return new TDynamicFunctionRegistry(*this);
    }

    void SetBackTraceCallback(NUdf::TBackTraceCallback callback) override {
        TWriteGuard guard(Lock_);
        BackTraceCallback_ = callback;
    }

private:
    const IBuiltinFunctionRegistry::TPtr Builtins_;

    mutable TRWMutex Lock_;
    THashMap<TString, TUdfLibraryPtr> LoadedLibraries_;
    TUdfModulesMap UdfModules_;
    TUdfModulePathsMap SystemModulePaths_;
    NUdf::TBackTraceCallback BackTraceCallback_ = nullptr;
    bool SupportsSizedAllocators_ = true;
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
