#include "module_catalog.h"

#include <util/generic/yexception.h>
#include <util/system/guard.h>

namespace NKikimr::NUdfStore::NWasm {

void TWasmModuleCatalog::Register(TWasmModuleArtifactPtr artifact) {
    Y_ENSURE(artifact);
    Y_ENSURE(!artifact->ModuleName.empty());

    with_lock (Mutex_) {
        ByModuleName_[artifact->ModuleName] = artifact;
    }
}

void TWasmModuleCatalog::Unregister(const TString& moduleName) {
    with_lock (Mutex_) {
        ByModuleName_.erase(moduleName);
    }
}

TWasmModuleArtifactPtr TWasmModuleCatalog::FindByModuleName(const TString& moduleName) const {
    with_lock (Mutex_) {
        if (auto it = ByModuleName_.find(moduleName); it != ByModuleName_.end()) {
            return it->second;
        }
    }
    return {};
}

TVector<TString> TWasmModuleCatalog::ListModuleNames() const {
    TVector<TString> result;
    with_lock (Mutex_) {
        result.reserve(ByModuleName_.size());
        for (const auto& [name, _] : ByModuleName_) {
            result.push_back(name);
        }
    }
    return result;
}

TVector<TWasmModuleArtifactPtr> TWasmModuleCatalog::ResolveModules(
    const TVector<TString>& moduleNames) const
{
    TVector<TWasmModuleArtifactPtr> result;
    result.reserve(moduleNames.size());
    with_lock (Mutex_) {
        for (const auto& name : moduleNames) {
            auto it = ByModuleName_.find(name);
            if (it == ByModuleName_.end()) {
                ythrow yexception() << "WASM UDF module is not loaded: " << name;
            }
            result.push_back(it->second);
        }
    }
    return result;
}

TWasmModuleCatalog& GetWasmModuleCatalog() {
    static TWasmModuleCatalog catalog;
    return catalog;
}

} // namespace NKikimr::NUdfStore::NWasm
