#pragma once

#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NKqp {

//! Hot-mutable UDF registry with RemoveModule (WASM / dynamic load).
//!
//! Concurrency model (actor-friendly):
//! - Readers (Find* / Get* / IsLoaded*) are wait-free via an immutable snapshot
//!   (THotSwap): no registry lock is held across module callbacks.
//! - Mutations (AddModule / RemoveModule / Set* / committing LoadUdfs) publish a
//!   new snapshot under a short writer lock (COW). Concurrent mutations from
//!   multiple threads/actors are supported; writers briefly serialize on publish.
//! - After RemoveModule publishes, new lookups do not observe the module
//!   (no phantom re-fetch). An in-flight call that already held shared_ptr may
//!   still finish — never use-after-free.
//! - LoadUdfs runs dlopen / Register under a separate load mutex (not on the
//!   reader path); do not hold actor mailboxes across that work when possible.
//! - Clone() copies the current snapshot; module Impl pointers are shared.
class IDynamicFunctionRegistry: public NMiniKQL::IMutableFunctionRegistry {
public:
    using TPtr = TIntrusivePtr<IDynamicFunctionRegistry>;

    //! Unloads a dynamically registered module by YQL module name. No-op if missing.
    //! Drops the LoadedLibraries entry when no modules from that path remain.
    //! Does not modify SystemModulePaths: FindUdfPath may still return a system
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
