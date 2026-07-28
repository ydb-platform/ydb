#pragma once

#include "compartment_manager.h"
#include "module_catalog.h"

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NWasm {

//! Keep only module names registered in the WASM catalog.
//! Stage predictor currently records every TCoUdf module (String, Knn, ...);
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

inline TVector<TString> CollectWasmUdfModules(
    const NYql::NDqProto::TProgram::TSettings& settings)
{
    TVector<TString> modules;
    modules.reserve(settings.WasmUdfModulesSize());
    for (const auto& module : settings.GetWasmUdfModules()) {
        modules.push_back(module);
    }
    return FilterLoadedWasmUdfModules(modules);
}

// Owns a per-query compartment. Install it as the current TLS compartment only
// for the duration of an activation guard (actor event / task run).
class TQueryCompartmentScope : public TNonCopyable {
public:
    explicit TQueryCompartmentScope(const NYql::NDqProto::TProgram::TSettings& settings) {
        const auto modules = CollectWasmUdfModules(settings);
        if (!modules.empty()) {
            Handle_ = GetWasmCompartmentManager().Acquire(modules);
        }
    }

    explicit TQueryCompartmentScope(const TVector<TString>& modules) {
        const auto loaded = FilterLoadedWasmUdfModules(modules);
        if (!loaded.empty()) {
            Handle_ = GetWasmCompartmentManager().Acquire(loaded);
        }
    }

    bool Active() const {
        return Handle_ != nullptr;
    }

    TCurrentQueryCompartmentGuard Activate() const {
        return TCurrentQueryCompartmentGuard(Handle_.get());
    }

private:
    TQueryCompartmentHandlePtr Handle_;
};

} // namespace NKikimr::NUdfStore::NWasm
