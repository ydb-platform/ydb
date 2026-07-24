#include "compartment_manager.h"

#include "host.h"
#include "registry_helpers.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NUdfStore::NWasm {

namespace {

thread_local TQueryCompartmentHandle* CurrentQueryCompartment = nullptr;

} // namespace

TString MakeExportKey(TStringBuf moduleName, TStringBuf functionName) {
    return TStringBuilder() << moduleName << "::" << functionName;
}

TQueryCompartmentHandlePtr TWasmCompartmentManager::Acquire(
    const TVector<TString>& moduleNames) const
{
    EnsureUdfHostIntrinsicsRegistered();

    if (moduleNames.empty()) {
        return {};
    }

    const auto artifacts = Catalog_.ResolveModules(moduleNames);

    THashSet<TString> loadedLibraries;
    TVector<TNamedModuleBytecode> libraries;
    for (const auto& artifact : artifacts) {
        // Index catalog libraries by name, then emit them in RequiredLibraries
        // order so sdk (env) is always linked before the UDF module below.
        THashMap<TString, TNamedModuleBytecode> byName;
        for (const auto& library : artifact->Libraries) {
            if (library.Name.empty()) {
                ythrow yexception()
                    << "WASM UDF '" << artifact->ModuleName
                    << "' has a library entry with empty name";
            }
            if (!library.Bytecode.ObjectCode) {
                ythrow yexception()
                    << "WASM UDF '" << artifact->ModuleName
                    << "' is missing object code for required library '"
                    << library.Name << "'";
            }
            byName.emplace(library.Name, library);
        }
        for (const auto& required : artifact->Manifest.RequiredLibraries) {
            const auto* library = byName.FindPtr(required);
            if (!library) {
                ythrow yexception()
                    << "WASM UDF '" << artifact->ModuleName
                    << "' requires library '" << required
                    << "' but it was not loaded into the module catalog";
            }
            if (loadedLibraries.insert(required).second) {
                libraries.push_back(*library);
            }
        }
    }

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    // CreateRegistryCompartment installs the first library via AddSdk ("env");
    // only then do we AddPrecompiledModule the UDF (e.g. Md5).
    handle->Compartment = CreateRegistryCompartment(libraries);

    for (const auto& artifact : artifacts) {
        AddPrecompiledModule(
            handle->Compartment.get(),
            artifact->ModuleBytecode,
            artifact->ModuleName);

        for (const auto& function : artifact->Manifest.Functions) {
            const auto key = MakeExportKey(artifact->ModuleName, function.Name);
            auto* exportPtr = handle->Compartment->GetFunction(std::string(function.Name));
            if (!exportPtr) {
                ythrow yexception()
                    << "Missing WASM export '" << function.Name
                    << "' in module '" << artifact->ModuleName << "'";
            }
            if (!handle->Exports.emplace(key, exportPtr).second) {
                ythrow yexception()
                    << "Duplicate WASM export key '" << key << "'";
            }
        }
    }

    return handle;
}

TWasmCompartmentManager& GetWasmCompartmentManager() {
    static TWasmCompartmentManager manager;
    return manager;
}

TCurrentQueryCompartmentGuard::TCurrentQueryCompartmentGuard(TQueryCompartmentHandle* handle)
    : Previous_(CurrentQueryCompartment)
    , Active_(true)
{
    CurrentQueryCompartment = handle;
}

TCurrentQueryCompartmentGuard::~TCurrentQueryCompartmentGuard() {
    if (Active_) {
        CurrentQueryCompartment = Previous_;
    }
}

TCurrentQueryCompartmentGuard::TCurrentQueryCompartmentGuard(
    TCurrentQueryCompartmentGuard&& other) noexcept
    : Previous_(other.Previous_)
    , Active_(other.Active_)
{
    other.Active_ = false;
}

TCurrentQueryCompartmentGuard& TCurrentQueryCompartmentGuard::operator=(
    TCurrentQueryCompartmentGuard&& other) noexcept
{
    if (this != &other) {
        if (Active_) {
            CurrentQueryCompartment = Previous_;
        }
        Previous_ = other.Previous_;
        Active_ = other.Active_;
        other.Active_ = false;
    }
    return *this;
}

TQueryCompartmentHandle* GetCurrentQueryCompartment() {
    return CurrentQueryCompartment;
}

} // namespace NKikimr::NUdfStore::NWasm
