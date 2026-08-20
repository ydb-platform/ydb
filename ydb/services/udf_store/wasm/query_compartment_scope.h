#pragma once

#include "compartment_manager.h"
#include "module_catalog.h"

#include <ydb/library/wasm/api/allocation_registry.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/join.h>
#include <util/string/split.h>

namespace NKikimr::NUdfStore::NWasm {

//! TaskParams key for per-stage WASM module list (KQP → CA).
//! Value: newline-separated module names from TKqpPhyStage.WasmUdfModules.
inline constexpr TStringBuf WasmUdfModulesTaskParam = "_WasmUdfModules";

inline TString SerializeWasmUdfModulesTaskParam(const TVector<TString>& modules) {
    return JoinSeq('\n', modules);
}

inline TVector<TString> ParseWasmUdfModulesTaskParam(TStringBuf data) {
    TVector<TString> modules;
    StringSplitter(data).Split('\n').SkipEmpty().Collect(&modules);
    return modules;
}

//! Keep only module names registered in the WASM catalog.
//! Stage predictor records every TCoUdf module (String, Knn, ...);
//! native UDFs must not trigger Acquire / ResolveModules.
inline TVector<TString> FilterLoadedWasmUdfModules(
    const TVector<TString>& modules,
    const TWasmModuleCatalog& catalog = GetWasmModuleCatalog())
{
    TVector<TString> result;
    result.reserve(modules.size());
    for (const auto& module : modules) {
        if (catalog.FindByModuleName(module)) {
            result.push_back(module);
        }
    }
    return result;
}

template <typename TRepeatedString>
inline TVector<TString> WasmUdfModulesFromRepeated(const TRepeatedString& repeated) {
    TVector<TString> modules;
    modules.reserve(repeated.size());
    for (const auto& module : repeated) {
        modules.push_back(module);
    }
    return FilterLoadedWasmUdfModules(modules);
}

// Holds a per-query compartment. Install it as the current TLS compartment only
// for the duration of a TLS guard (actor event / task run).
//
// The scope is not the sole owner: strings materialized into linear memory are
// destroyed by whoever holds the last reference to them, which can happen after
// the scope is gone (a compute actor is destroyed before its task runner tears
// the computation graph down). Those values keep the compartment alive through
// TWasmAllocationRegistry.
class TQueryCompartmentScope : public TNonCopyable {
public:
    explicit TQueryCompartmentScope(const TVector<TString>& modules) {
        const auto loaded = FilterLoadedWasmUdfModules(modules);
        if (!loaded.empty()) {
            Handle_ = GetWasmCompartmentManager().Acquire(loaded);
            NYdb::NWasm::TWasmAllocationRegistry::Instance().RetainOwner(
                Handle_->Generation, Handle_);
        }
    }

    ~TQueryCompartmentScope() {
        if (Handle_) {
            NYdb::NWasm::TWasmAllocationRegistry::Instance().ReleaseOwner(Handle_->Generation);
        }
    }

    bool HasHandle() const {
        return Handle_ != nullptr;
    }

    //! What the resident string path actually did in this query: how many column
    //! values went into linear memory, how many UDF args reused those bytes, and
    //! how many still had to be copied per call.
    TPreferWasmCounters::TSnapshot GetPreferWasmSnapshot() const {
        return Handle_ ? Handle_->PreferWasm.GetSnapshot() : TPreferWasmCounters::TSnapshot{};
    }

    TCurrentQueryCompartmentGuard MakeTlsGuard() const {
        return TCurrentQueryCompartmentGuard(Handle_.get());
    }

private:
    TQueryCompartmentHandlePtr Handle_;
};

} // namespace NKikimr::NUdfStore::NWasm
