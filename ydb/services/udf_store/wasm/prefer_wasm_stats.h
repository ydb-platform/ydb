#pragma once

#include <util/system/types.h>

#include <atomic>

namespace NKikimr::NUdfStore::NWasm {

////////////////////////////////////////////////////////////////////////////////

//! Process-wide counters of the PreferWasm path: a string column materialized
//! straight into WASM linear memory (1 copy) instead of a host string that every
//! UDF call has to copy into the compartment.
//!
//! FallbackNoCompartment > 0 means columns were marked for a stage that has no
//! query compartment, i.e. the read and the UDF ended up in different tasks.
class TPreferWasmStats {
public:
    struct TSnapshot {
        ui64 ColumnsMarked = 0;
        ui64 MaterializedInWasm = 0;
        ui64 FallbackNoCompartment = 0;
        ui64 ResidentReused = 0;
        ui64 CopiedIntoCompartment = 0;
    };

    static TPreferWasmStats& Instance();

    void OnColumnsMarked(ui64 count) {
        ColumnsMarked_.fetch_add(count, std::memory_order_relaxed);
    }

    void OnMaterializedInWasm() {
        MaterializedInWasm_.fetch_add(1, std::memory_order_relaxed);
    }

    //! Also logs once per process: marking without a compartment is a planning bug.
    void OnFallbackNoCompartment();

    void OnResidentReused() {
        ResidentReused_.fetch_add(1, std::memory_order_relaxed);
    }

    void OnCopiedIntoCompartment() {
        CopiedIntoCompartment_.fetch_add(1, std::memory_order_relaxed);
    }

    TSnapshot GetSnapshot() const;
    void Reset();

private:
    std::atomic<ui64> ColumnsMarked_{0};
    std::atomic<ui64> MaterializedInWasm_{0};
    std::atomic<ui64> FallbackNoCompartment_{0};
    std::atomic<ui64> ResidentReused_{0};
    std::atomic<ui64> CopiedIntoCompartment_{0};
    std::atomic<bool> FallbackReported_{false};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NKikimr::NUdfStore::NWasm
