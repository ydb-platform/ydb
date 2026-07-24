#pragma once

#include "module_catalog.h"

#include <ydb/library/wasm/api/compartment.h>

#include <util/generic/hash.h>
#include <util/generic/noncopyable.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NKikimr::NUdfStore::NWasm {

struct TQueryCompartmentHandle : public TNonCopyable {
    std::unique_ptr<NYdb::NWasm::IWebAssemblyCompartment> Compartment;
    // Key: "ModuleName::ExportName" (YQL name or create/call/destroy export)
    THashMap<TString, void*> Exports;
    // Monotonic id for this acquire; TypeConfig callables recreate objects on change.
    ui64 Generation = 0;
};

using TQueryCompartmentHandlePtr = std::unique_ptr<TQueryCompartmentHandle>;

class TWasmCompartmentManager {
public:
    explicit TWasmCompartmentManager(TWasmModuleCatalog& catalog = GetWasmModuleCatalog())
        : Catalog_(catalog)
    {}

    TQueryCompartmentHandlePtr Acquire(const TVector<TString>& moduleNames) const;

private:
    TWasmModuleCatalog& Catalog_;
};

TWasmCompartmentManager& GetWasmCompartmentManager();

class TCurrentQueryCompartmentGuard {
public:
    explicit TCurrentQueryCompartmentGuard(TQueryCompartmentHandle* handle);
    ~TCurrentQueryCompartmentGuard();

    TCurrentQueryCompartmentGuard(const TCurrentQueryCompartmentGuard&) = delete;
    TCurrentQueryCompartmentGuard& operator=(const TCurrentQueryCompartmentGuard&) = delete;

    TCurrentQueryCompartmentGuard(TCurrentQueryCompartmentGuard&& other) noexcept;
    TCurrentQueryCompartmentGuard& operator=(TCurrentQueryCompartmentGuard&& other) noexcept;

private:
    TQueryCompartmentHandle* Previous_ = nullptr;
    bool Active_ = false;
};

TQueryCompartmentHandle* GetCurrentQueryCompartment();

TString MakeExportKey(TStringBuf moduleName, TStringBuf functionName);

} // namespace NKikimr::NUdfStore::NWasm
