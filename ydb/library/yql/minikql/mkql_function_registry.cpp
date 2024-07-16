#include "mkql_function_registry.h"
#include "mkql_utils.h"
#include "mkql_type_builder.h"
#include <ydb/library/yql/public/udf/udf_static_registry.h>

#include <util/folder/iterator.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/system/dynlib.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/split.h>

namespace {

using namespace NKikimr;
using namespace NMiniKQL;

const char MODULE_NAME_DELIMITER = '.';
const char* RegisterFuncName = "Register";
const char* AbiVersionFuncName = "AbiVersion";
#if defined(_win_) || defined(_darwin_)
const char* BindSymbolsFuncName = "BindSymbols";
#endif
const char* SetBackTraceCallbackName = "SetBackTraceCallback";

//////////////////////////////////////////////////////////////////////////////
// TMutableFunctionRegistry
//////////////////////////////////////////////////////////////////////////////
class TMutableFunctionRegistry: public IMutableFunctionRegistry
{
    struct TUdfModule {
        TString LibraryPath;
        std::shared_ptr<NUdf::IUdfModule> Impl;
    };

    using TUdfModulesMap = THashMap<TString, TUdfModule>;

    struct TUdfLibrary: public TThrRefBase {
        ui32 AbiVersion = 0;
        TDynamicLibrary Lib;
        TUdfLibrary()
        {
        }
    };
    using TUdfLibraryPtr = TIntrusivePtr<TUdfLibrary>;

    class TUdfModuleLoader: public NUdf::IRegistrator {
    public:
        TUdfModuleLoader(
                TUdfModulesMap& modulesMap,
                THashSet<TString>* newModules,
                const TString& libraryPath,
                const TUdfModuleRemappings& remappings,
                ui32 abiVersion,
                const TString& customUdfPrefix = {})
            : ModulesMap(modulesMap)
            , NewModules(newModules)
            , LibraryPath(libraryPath)
            , Remappings(remappings)
            , AbiVersion(NUdf::AbiVersionToStr(abiVersion))
            , CustomUdfPrefix(customUdfPrefix)
        {
        }

        void AddModule(
                const NUdf::TStringRef& name,
                NUdf::TUniquePtr<NUdf::IUdfModule> module) override
        {
            Y_DEBUG_ABORT_UNLESS(module, "Module is empty");

            if (!HasError()) {
                TUdfModule m;
                m.LibraryPath = LibraryPath;
                m.Impl.reset(module.Release());

                auto it = Remappings.find(name);
                const TString& newName = CustomUdfPrefix
                        + ((it == Remappings.end())
                        ? TString(name)
                        : it->second);

                auto i = ModulesMap.insert({ newName, std::move(m) });
                if (!i.second) {
                    TUdfModule* oldModule = ModulesMap.FindPtr(newName);
                    Y_DEBUG_ABORT_UNLESS(oldModule != nullptr);
                    Error = (TStringBuilder()
                             << "UDF module duplication: name " << TStringBuf(name)
                             << ", already loaded from " << oldModule->LibraryPath
                             << ", trying to load from " << LibraryPath);
                } else if (NewModules) {
                    NewModules->insert(newName);
                }
            }
        }

        const TString& GetError() const { return Error; }
        bool HasError() const { return !Error.empty(); }

    private:
        TUdfModulesMap& ModulesMap;
        THashSet<TString>* NewModules;
        const TString LibraryPath;
        const TUdfModuleRemappings& Remappings;
        const TString AbiVersion;
        TString Error;
        const TString CustomUdfPrefix;
    };

public:
    TMutableFunctionRegistry(IBuiltinFunctionRegistry::TPtr builtins)
        : Builtins_(std::move(builtins))
    {
    }

    TMutableFunctionRegistry(const TMutableFunctionRegistry& rhs)
        : Builtins_(rhs.Builtins_)
        , LoadedLibraries_(rhs.LoadedLibraries_)
        , UdfModules_(rhs.UdfModules_)
        , SupportsSizedAllocators_(rhs.SupportsSizedAllocators_)
    {
    }

    ~TMutableFunctionRegistry() {
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

            // (1) check ABI version
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

            libIt = LoadedLibraries_.insert({ libraryPath, lib }).first;
        } else {
            lib = libIt->second;
        }

        // (2) ensure that Register() func is present
        auto registerFunc = reinterpret_cast<NUdf::TRegisterFunctionPtr>(
                    lib->Lib.Sym(RegisterFuncName));

        // (3) do load
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
        TString libraryPathStr(libraryPath);
        auto inserted = LoadedLibraries_.insert({ libraryPathStr, nullptr });
        if (!inserted.second) {
            return;
        }

        TUdfModuleRemappings remappings;
        TUdfModuleLoader loader(
                    UdfModules_, nullptr, libraryPathStr,
                    remappings, NUdf::CurrentAbiVersion());
        loader.AddModule(moduleName, std::move(module));

        Y_ENSURE(!loader.HasError(), loader.GetError());
    }

    void SetSystemModulePaths(const TUdfModulePathsMap& paths) override {
        SystemModulePaths_ = paths;
    }

    const IBuiltinFunctionRegistry::TPtr& GetBuiltins() const override {
        return Builtins_;
    }

    TStatus FindFunctionTypeInfo(
            const TTypeEnvironment& env,
            NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
            NUdf::ICountersProvider* countersProvider,
            const TStringBuf& name,
            TType* userType,
            const TStringBuf& typeConfig,
            ui32 flags,
            const NUdf::TSourcePosition& pos,
            const NUdf::ISecureParamsProvider* secureParamsProvider,
            TFunctionTypeInfo* funcInfo) const override
    {
        TStringBuf moduleName, funcName;
        if (name.TrySplit(MODULE_NAME_DELIMITER, moduleName, funcName)) {
            auto it = UdfModules_.find(moduleName);
            if (it != UdfModules_.end()) {
                TFunctionTypeInfoBuilder typeInfoBuilder(env, typeInfoHelper, moduleName,
                    (flags & NUdf::IUdfModule::TFlags::TypesOnly) ? nullptr : countersProvider, pos, secureParamsProvider);
                const auto& module = *it->second.Impl;
                module.BuildFunctionTypeInfo(
                    funcName, userType, typeConfig, flags, typeInfoBuilder);

                if (typeInfoBuilder.HasError()) {
                    return TStatus::Error()
                            << "Module: " << moduleName
                            << ", function: " << funcName
                            << ", error: " << typeInfoBuilder.GetError();
                }

                try {
                    typeInfoBuilder.Build(funcInfo);
                }
                catch (yexception& e) {
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

            return TStatus::Error()
                    << "Module " << moduleName << " is not registered";
        }

        return TStatus::Error()
                << "Function name must be in <module>.<func_name> scheme. "
                << "But get " << name;
    }

    TMaybe<TString> FindUdfPath(const TStringBuf& moduleName) const override {
        if (const TUdfModule* udf = UdfModules_.FindPtr(moduleName)) {
            return udf->LibraryPath;
        }

        if (const TString* path = SystemModulePaths_.FindPtr(moduleName)) {
            return *path;
        }

        return Nothing();
    }

    bool IsLoadedUdfModule(const TStringBuf& moduleName) const override {
        return UdfModules_.contains(moduleName);
    }

    THashSet<TString> GetAllModuleNames() const override {
        THashSet<TString> names;
        names.reserve(UdfModules_.size());
        for (const auto& module: UdfModules_) {
            names.insert(module.first);
        }
        return names;
    }

    TFunctionsMap GetModuleFunctions(const TStringBuf& moduleName) const override {
        struct TFunctionNamesSink: public NUdf::IFunctionNamesSink {
            TFunctionsMap Functions;
            class TFuncDescriptor : public NUdf::IFunctionDescriptor {
            public:
                TFuncDescriptor(TFunctionProperties& properties)
                    : Properties(properties)
                {}

            private:
                void SetTypeAwareness() final {
                    Properties.IsTypeAwareness = true;
                }

                TFunctionProperties& Properties;
            };

            NUdf::IFunctionDescriptor::TPtr Add(const NUdf::TStringRef& name) final {
                const auto it = Functions.emplace(name, TFunctionProperties{});
                return new TFuncDescriptor(it.first->second);
            }
        } sink;


        const auto it = UdfModules_.find(moduleName);
        if (UdfModules_.cend() == it)
            return TFunctionsMap();
        it->second.Impl->GetAllFunctions(sink);
        return sink.Functions;
    }

    bool SupportsSizedAllocators() const override {
        return SupportsSizedAllocators_;
    }

    void PrintInfoTo(IOutputStream& out) const override {
        Builtins_->PrintInfoTo(out);
    }

    void CleanupModulesOnTerminate() const override {
        for (const auto& module : UdfModules_) {
            module.second.Impl->CleanupOnTerminate();
        }
    }

    TIntrusivePtr<IMutableFunctionRegistry> Clone() const override {
        return new TMutableFunctionRegistry(*this);
    }

    void SetBackTraceCallback(NUdf::TBackTraceCallback callback) override {
        BackTraceCallback_ = callback;
    }

private:
    const IBuiltinFunctionRegistry::TPtr Builtins_;

    THashMap<TString, TUdfLibraryPtr> LoadedLibraries_;
    TUdfModulesMap UdfModules_;
    THolder<TMemoryUsageInfo> UdfMemoryInfo_;
    TUdfModulePathsMap SystemModulePaths_;
    NUdf::TBackTraceCallback BackTraceCallback_ = nullptr;
    bool SupportsSizedAllocators_ = true;
};

//////////////////////////////////////////////////////////////////////////////
// TBuiltinsWrapper
//////////////////////////////////////////////////////////////////////////////
class TBuiltinsWrapper: public IFunctionRegistry
{
public:
    TBuiltinsWrapper(IBuiltinFunctionRegistry::TPtr&& builtins)
        : Builtins_(std::move(builtins))
    {
    }

    const IBuiltinFunctionRegistry::TPtr& GetBuiltins() const override {
        return Builtins_;
    }

    void AllowUdfPatch() override {
    }

    TStatus FindFunctionTypeInfo(
            const TTypeEnvironment& env,
            NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
            NUdf::ICountersProvider* countersProvider,
            const TStringBuf& name,
            TType* userType,
            const TStringBuf& typeConfig,
            ui32 flags,
            const NUdf::TSourcePosition& pos,
            const NUdf::ISecureParamsProvider* secureParamsProvider,
            TFunctionTypeInfo* funcInfo) const override
    {
        Y_UNUSED(env);
        Y_UNUSED(typeInfoHelper);
        Y_UNUSED(countersProvider);
        Y_UNUSED(name);
        Y_UNUSED(userType);
        Y_UNUSED(typeConfig);
        Y_UNUSED(flags);
        Y_UNUSED(pos);
        Y_UNUSED(secureParamsProvider);
        Y_UNUSED(funcInfo);
        return TStatus::Error(TStringBuf("Unsupported access to builtins registry"));
    }

    TMaybe<TString> FindUdfPath(
            const TStringBuf& /* moduleName */) const override
    {
        return{};
    }

    bool IsLoadedUdfModule(const TStringBuf& /* moduleName */) const override {
        return false;
    }

    THashSet<TString> GetAllModuleNames() const override {
        return {};
    }

    TFunctionsMap GetModuleFunctions(const TStringBuf&) const override {
        return TFunctionsMap();
    }

    bool SupportsSizedAllocators() const override {
        return true;
    }

    void PrintInfoTo(IOutputStream& out) const override {
        Builtins_->PrintInfoTo(out);
    }

    void CleanupModulesOnTerminate() const override {
    }

    TIntrusivePtr<IMutableFunctionRegistry> Clone() const override {
        return new TMutableFunctionRegistry(Builtins_);
    }

private:
    const IBuiltinFunctionRegistry::TPtr Builtins_;
};

} // namespace

namespace NKikimr {
namespace NMiniKQL {

void FindUdfsInDir(const TString& dirPath, TVector<TString>* paths)
{
    static const TStringBuf libPrefix = TStringBuf(MKQL_UDF_LIB_PREFIX);
    static const TStringBuf libSuffix = TStringBuf(MKQL_UDF_LIB_SUFFIX);

    if (!dirPath.empty()) {
        std::vector<TString> dirs;
        StringSplitter(dirPath).Split(';').AddTo(&dirs);

        for (auto d : dirs) {
            TDirIterator dir(d, TDirIterator::TOptions(FTS_LOGICAL).SetMaxLevel(10));

            for (auto file = dir.begin(), end = dir.end(); file != end; ++file) {
                // skip entries with empty name, and all non-files
                // all valid symlinks are already dereferenced, provided by FTS_LOGICAL
                if (file->fts_pathlen == file->fts_namelen || file->fts_info != FTS_F) {
                    continue;
                }

                TString path(file->fts_path);
                TString fileName = GetBaseName(path);

                // skip non shared libraries
                if (!fileName.StartsWith(libPrefix) ||
                    !fileName.EndsWith(libSuffix))
                {
                    continue;
                }

                // skip test udfs when scanning dir
                auto udfName = TStringBuf(fileName).Skip(libPrefix.length());
                if (udfName.StartsWith(TStringBuf("test_"))) {
                    continue;
                }

                paths->push_back(std::move(path));
            }
        }
    }
}

bool SplitModuleAndFuncName(const TStringBuf& name, TStringBuf& module, TStringBuf& func)
{
    return name.TrySplit(MODULE_NAME_DELIMITER, module, func);
}

TString FullName(const TStringBuf& module, const TStringBuf& func)
{
    TString fullName;
    fullName.reserve(module.size() + func.size() + 1);
    fullName.append(module);
    fullName.append(MODULE_NAME_DELIMITER);
    fullName.append(func);
    return fullName;
}

TIntrusivePtr<IFunctionRegistry> CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr&& builtins)
{
    return new TBuiltinsWrapper(std::move(builtins));
}

TIntrusivePtr<IFunctionRegistry> CreateFunctionRegistry(
    NKikimr::NUdf::TBackTraceCallback backtraceCallback,
    IBuiltinFunctionRegistry::TPtr&& builtins,
    bool allowUdfPatch,
    const TVector<TString>& udfsPaths,
    ui32 flags /* = 0 */)
{
    auto registry = MakeHolder<TMutableFunctionRegistry>(std::move(builtins));
    if (allowUdfPatch) {
        registry->AllowUdfPatch();
    }
    registry->SetBackTraceCallback(backtraceCallback);

    // system UDFs loaded with default names
    TUdfModuleRemappings remappings;
    THashSet<TString> usedUdfPaths;
    for (const TString& udfPath: udfsPaths) {
        if (usedUdfPaths.insert(udfPath).second) {
            registry->LoadUdfs(udfPath, remappings, flags);
        }
    }

    return registry.Release();
}

void FillStaticModules(IMutableFunctionRegistry& registry) {
    for (const auto& wrapper : NUdf::GetStaticUdfModuleWrapperList()) {
        auto [name, ptr] = wrapper();
        registry.AddModule(TString(StaticModulePrefix) + name, name, std::move(ptr));
    }
}

} // namespace NMiniKQL
} // namespace NKiki
