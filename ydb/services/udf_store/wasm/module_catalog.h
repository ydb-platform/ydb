#pragma once

#include "manifest.h"
#include "registry_helpers.h"

#include <ydb/library/wasm/api/bytecode.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <memory>

namespace NKikimr::NUdfStore::NWasm {

struct TWasmModuleArtifact {
    TString ModuleName;
    TWasmManifest Manifest;
    NYdb::NWasm::TModuleBytecode ModuleBytecode;
    TVector<TNamedModuleBytecode> Libraries;
};

using TWasmModuleArtifactPtr = std::shared_ptr<const TWasmModuleArtifact>;

//! Modules currently loaded on this node, keyed by the name YQL calls them by.
//! A name identifies exactly one module, so registering replaces whatever was
//! loaded under that name before.
class TWasmModuleCatalog {
public:
    void Register(TWasmModuleArtifactPtr artifact);
    void Unregister(const TString& moduleName);

    TWasmModuleArtifactPtr FindByModuleName(const TString& moduleName) const;

    //! Names of all modules currently registered in the catalog.
    TVector<TString> ListModuleNames() const;

    TVector<TWasmModuleArtifactPtr> ResolveModules(const TVector<TString>& moduleNames) const;

private:
    mutable TMutex Mutex_;
    THashMap<TString, TWasmModuleArtifactPtr> ByModuleName_;
};

TWasmModuleCatalog& GetWasmModuleCatalog();

} // namespace NKikimr::NUdfStore::NWasm
