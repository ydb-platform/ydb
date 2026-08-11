#include "module_catalog.h"

#include <util/generic/yexception.h>
#include <util/system/guard.h>

namespace NKikimr::NUdfStore::NWasm {

void TWasmModuleCatalog::Register(TWasmModuleArtifactPtr artifact) {
    Y_ENSURE(artifact);
    Y_ENSURE(!artifact->Md5.empty());
    Y_ENSURE(!artifact->ModuleName.empty());

    with_lock (Mutex_) {
        if (auto it = ByMd5_.find(artifact->Md5); it != ByMd5_.end()) {
            ByModuleName_.erase(it->second->ModuleName);
        }
        ByMd5_[artifact->Md5] = artifact;
        ByModuleName_[artifact->ModuleName] = artifact;
    }
}

void TWasmModuleCatalog::Unregister(const TString& md5) {
    with_lock (Mutex_) {
        auto it = ByMd5_.find(md5);
        if (it == ByMd5_.end()) {
            return;
        }
        ByModuleName_.erase(it->second->ModuleName);
        ByMd5_.erase(it);
    }
}

TWasmModuleArtifactPtr TWasmModuleCatalog::FindByMd5(const TString& md5) const {
    with_lock (Mutex_) {
        if (auto it = ByMd5_.find(md5); it != ByMd5_.end()) {
            return it->second;
        }
    }
    return {};
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
