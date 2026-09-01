#pragma once

#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NKqp {

class IDynamicFunctionRegistry: public NMiniKQL::IMutableFunctionRegistry {
public:
    using TPtr = TIntrusivePtr<IDynamicFunctionRegistry>;

    //! Unloads a dynamically registered module by YQL module name. No-op if missing.
    //! Drops the LoadedLibraries_ entry when no modules from that path remain.
    //! Does not modify SystemModulePaths_: FindUdfPath may still return a system
    //! catalog path after unload (same as for never-loaded system modules).
    virtual void RemoveModule(const TStringBuf& moduleName) = 0;
};

//! Creates a dynamic registry (full mutable UDF registry + RemoveModule).
//! Returned as IMutableFunctionRegistry; cast to IDynamicFunctionRegistry for RemoveModule.
TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> CreateDynamicFunctionRegistry(
    NMiniKQL::IBuiltinFunctionRegistry::TPtr&& builtins);

TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> CreateDynamicFunctionRegistry(
    NKikimr::NUdf::TBackTraceCallback backtraceCallback,
    NMiniKQL::IBuiltinFunctionRegistry::TPtr&& builtins,
    bool allowUdfPatch,
    const TVector<TString>& udfsPaths,
    ui32 flags = 0);

inline IDynamicFunctionRegistry* AsDynamicFunctionRegistry(
    NMiniKQL::IMutableFunctionRegistry* registry)
{
    return dynamic_cast<IDynamicFunctionRegistry*>(registry);
}

} // namespace NKikimr::NKqp
