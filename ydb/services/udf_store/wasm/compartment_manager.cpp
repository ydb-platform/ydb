#include "compartment_manager.h"

#include "host.h"
#include "registry_helpers.h"
#include "types.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <atomic>
#include <string>

namespace NKikimr::NUdfStore::NWasm {

namespace {

thread_local TQueryCompartmentHandle* CurrentQueryCompartment = nullptr;

ui64 NextCompartmentGeneration() {
    static std::atomic<ui64> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

void BindExport(
    TQueryCompartmentHandle& handle,
    const TString& moduleName,
    const TString& exportName)
{
    if (exportName.empty()) {
        return;
    }
    const auto key = MakeExportKey(moduleName, exportName);
    if (handle.Exports.contains(key)) {
        return;
    }
    auto* exportPtr = handle.Compartment->GetFunction(std::string(exportName));
    if (!exportPtr) {
        ythrow yexception()
            << "Missing WASM export '" << exportName
            << "' in module '" << moduleName << "'";
    }
    handle.Exports.emplace(key, exportPtr);
}

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
    handle->Generation = NextCompartmentGeneration();
    // CreateRegistryCompartment clones SDK from CreateImageFromSdk cache ("env");
    // only then do we AddPrecompiledModule the UDF (e.g. Md5).
    handle->Compartment = CreateRegistryCompartment(libraries);

    for (const auto& artifact : artifacts) {
        AddPrecompiledModule(
            handle->Compartment.get(),
            artifact->ModuleBytecode,
            artifact->ModuleName);

        for (const auto& function : artifact->Manifest.Functions) {
            if (function.Binding == EWasmUdfBinding::TypeConfigCallable) {
                BindExport(*handle, artifact->ModuleName, function.CreateExport);
                BindExport(*handle, artifact->ModuleName, function.CallExport);
                BindExport(*handle, artifact->ModuleName, function.DestroyExport);
            } else {
                BindExport(*handle, artifact->ModuleName, TString(PlainWasmExport(function)));
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
