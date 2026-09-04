#pragma once

#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NKqp {

//! Thread-safe mutable UDF registry with RemoveModule.
//!
//! Contract for a single instance:
//! - All methods may be called concurrently from multiple threads.
//! - Mutations (LoadUdfs / AddModule / RemoveModule / Set*) are serialized.
//! - Concurrent Find* / Get* during RemoveModule either observe the module
//!   (and keep it alive via shared_ptr for the duration of the module call)
//!   or get "not registered" — never use-after-free.
//! - Clone() takes a consistent snapshot under a shared lock; the clone is
//!   independent of the source.
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
//! The returned instance is thread-safe (see IDynamicFunctionRegistry).
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
