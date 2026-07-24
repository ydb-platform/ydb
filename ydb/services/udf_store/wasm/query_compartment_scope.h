#pragma once

#include "compartment_manager.h"

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <util/generic/vector.h>

namespace NKikimr::NUdfStore::NWasm {

inline TVector<TString> CollectWasmUdfModules(
    const NYql::NDqProto::TProgram::TSettings& settings)
{
    TVector<TString> modules;
    modules.reserve(settings.WasmUdfModulesSize());
    for (const auto& module : settings.GetWasmUdfModules()) {
        modules.push_back(module);
    }
    return modules;
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
        if (!modules.empty()) {
            Handle_ = GetWasmCompartmentManager().Acquire(modules);
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
