#pragma once

#include "mkql_function_metadata.h"

#include <ydb/library/yql/public/udf/udf_counter.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <map>

#ifdef _win_
#   define MKQL_UDF_LIB_PREFIX ""
#   define MKQL_UDF_LIB_SUFFIX ".dll"
#elif defined(_darwin_)
#   define MKQL_UDF_LIB_PREFIX "lib"
#   define MKQL_UDF_LIB_SUFFIX ".dylib"
#else
#   define MKQL_UDF_LIB_PREFIX "lib"
#   define MKQL_UDF_LIB_SUFFIX ".so"
#endif

//////////////////////////////////////////////////////////////////////////////
// forward declarations
//////////////////////////////////////////////////////////////////////////////
class IOutputStream;

namespace NKikimr {
    namespace NMiniKQL {
        class IMutableFunctionRegistry;
        class TTypeEnvironment;
        struct TFunctionTypeInfo;
        class TStatus;
        class TType;
    }
}

namespace NKikimr {
namespace NMiniKQL {

using TUdfModuleRemappings = THashMap<TString, TString>; // old => new
using TUdfModulePathsMap = THashMap<TString, TString>; // module name => udf path

//////////////////////////////////////////////////////////////////////////////
// IFunctionRegistry
//////////////////////////////////////////////////////////////////////////////
class IFunctionRegistry: public TThrRefBase
{
public:
    typedef TIntrusivePtr<IFunctionRegistry> TPtr;

    virtual ~IFunctionRegistry() = default;

    virtual const IBuiltinFunctionRegistry::TPtr& GetBuiltins() const = 0;

    virtual void AllowUdfPatch() = 0;

    virtual TStatus FindFunctionTypeInfo(
            const TTypeEnvironment& env,
            NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
            NUdf::ICountersProvider* countersProvider,
            const TStringBuf& name,
            TType* userType,
            const TStringBuf& typeConfig,
            ui32 flags,
            const NUdf::TSourcePosition& pos,
            const NUdf::ISecureParamsProvider* secureParamsProvider,
            TFunctionTypeInfo* funcInfo) const = 0;

    virtual TMaybe<TString> FindUdfPath(const TStringBuf& moduleName) const = 0;

    virtual bool IsLoadedUdfModule(const TStringBuf& moduleName) const = 0;

    virtual THashSet<TString> GetAllModuleNames() const = 0;

    virtual void PrintInfoTo(IOutputStream& out) const = 0;

    virtual void CleanupModulesOnTerminate() const = 0;

    virtual TIntrusivePtr<IMutableFunctionRegistry> Clone() const = 0;

    struct TFunctionProperties { bool IsTypeAwareness = false; };

    typedef std::map<TString, TFunctionProperties> TFunctionsMap;

    virtual TFunctionsMap GetModuleFunctions(const TStringBuf& moduleName) const = 0;

    virtual bool SupportsSizedAllocators() const = 0;
};

//////////////////////////////////////////////////////////////////////////////
// IMutableFunctionRegistry
//////////////////////////////////////////////////////////////////////////////
class IMutableFunctionRegistry: public IFunctionRegistry
{
public:
    virtual void SetBackTraceCallback(NUdf::TBackTraceCallback callback) = 0;

    virtual void LoadUdfs(
            const TString& libraryPath,
            const TUdfModuleRemappings& remmapings,
            ui32 flags = 0,
            const TString& customUdfPrefix = {},
            THashSet<TString>* modules = nullptr) = 0;

    virtual void AddModule(
            const TStringBuf& libraryPath,
            const TStringBuf& moduleName,
            NUdf::TUniquePtr<NUdf::IUdfModule> module) = 0;

    virtual void SetSystemModulePaths(const TUdfModulePathsMap& paths) = 0;
};

//////////////////////////////////////////////////////////////////////////////
// factories
//////////////////////////////////////////////////////////////////////////////
TIntrusivePtr<IFunctionRegistry> CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr&& builtins);

TIntrusivePtr<IFunctionRegistry> CreateFunctionRegistry(
        NKikimr::NUdf::TBackTraceCallback backtraceCallback,
        IBuiltinFunctionRegistry::TPtr&& builtins,
        bool allowUdfPatch,
        const TVector<TString>& udfsPaths,
        ui32 flags = 0); // see NUdf::IRegistrator::TFlags

//////////////////////////////////////////////////////////////////////////////
// helper functions
//////////////////////////////////////////////////////////////////////////////
void FindUdfsInDir(const TString& dirPath, TVector<TString>* paths);

bool SplitModuleAndFuncName(
        const TStringBuf& name, TStringBuf& module, TStringBuf& func);
TString FullName(const TStringBuf& module, const TStringBuf& func);

inline TStringBuf ModuleName(const TStringBuf& name) {
    TStringBuf moduleName, _;
    if (SplitModuleAndFuncName(name, moduleName, _)) {
        return moduleName;
    }
    return name;
}

const TStringBuf StaticModulePrefix(TStringBuf("<static>::"));

void FillStaticModules(IMutableFunctionRegistry& registry);

} // namespace NMiniKQL
} // namespace NKikimr
