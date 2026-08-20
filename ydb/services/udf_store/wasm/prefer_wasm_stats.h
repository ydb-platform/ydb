#pragma once

#include <util/system/types.h>

#include <atomic>

namespace NKikimr::NUdfStore::NWasm {

////////////////////////////////////////////////////////////////////////////////

//! The same events as TPreferWasmStats, counted for one query compartment
//! (TQueryCompartmentHandle::PreferWasm). Process-wide totals cannot tell
//! whether a particular query used linear memory; a compute actor logs these.
class TPreferWasmCounters {
public:
    struct TSnapshot {
        ui64 MaterializedInWasm = 0;
        ui64 ResidentReused = 0;
        ui64 CopiedIntoCompartment = 0;
        ui64 ResidentConstArgs = 0;

        bool Empty() const {
            return MaterializedInWasm == 0 && ResidentReused == 0 && CopiedIntoCompartment == 0
                && ResidentConstArgs == 0;
        }
    };

    void OnMaterializedInWasm() {
        MaterializedInWasm_.fetch_add(1, std::memory_order_relaxed);
    }

    //! A loop-invariant UDF arg pinned into linear memory once via KqpWasmResidentString.
    void OnResidentConstArg() {
        ResidentConstArgs_.fetch_add(1, std::memory_order_relaxed);
    }

    void OnResidentReused() {
        ResidentReused_.fetch_add(1, std::memory_order_relaxed);
    }

    void OnCopiedIntoCompartment() {
        CopiedIntoCompartment_.fetch_add(1, std::memory_order_relaxed);
    }

    TSnapshot GetSnapshot() const;

private:
    std::atomic<ui64> MaterializedInWasm_{0};
    std::atomic<ui64> ResidentReused_{0};
    std::atomic<ui64> CopiedIntoCompartment_{0};
    std::atomic<ui64> ResidentConstArgs_{0};
};

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
