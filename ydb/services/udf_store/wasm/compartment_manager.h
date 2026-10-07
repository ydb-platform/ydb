#pragma once

#include "bridge_node_table.h"
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
    //! Host UnboxedValue nodes exposed to guest as bridge handles.
    std::unique_ptr<TWasmBridgeNodeTable> BridgeNodes;
    //! Nesting of BridgeRun calls. Lives here rather than on the invocation
    //! context because a callable can lead back into another WASM UDF, and
    //! that opens a fresh context whose own counter would start over.
    ui32 BridgeRunDepth = 0;
    //! Bytes materialized into linear memory for those nodes (pins, per-Run
    //! scratch, guest-owned blocks). Declared last: it holds compartment
    //! offsets and node values, so it must die before both.
    std::unique_ptr<TCompartmentResidentCache> Resident;
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
