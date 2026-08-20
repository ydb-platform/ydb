#include "prefer_wasm_stats.h"

#include <util/generic/singleton.h>
#include <util/stream/output.h>

namespace NKikimr::NUdfStore::NWasm {

////////////////////////////////////////////////////////////////////////////////

TPreferWasmCounters::TSnapshot TPreferWasmCounters::GetSnapshot() const {
    TSnapshot snapshot;
    snapshot.MaterializedInWasm = MaterializedInWasm_.load(std::memory_order_relaxed);
    snapshot.ResidentReused = ResidentReused_.load(std::memory_order_relaxed);
    snapshot.CopiedIntoCompartment = CopiedIntoCompartment_.load(std::memory_order_relaxed);
    snapshot.ResidentConstArgs = ResidentConstArgs_.load(std::memory_order_relaxed);
    return snapshot;
}

TPreferWasmStats& TPreferWasmStats::Instance() {
    return *Singleton<TPreferWasmStats>();
}

void TPreferWasmStats::OnFallbackNoCompartment() {
    FallbackNoCompartment_.fetch_add(1, std::memory_order_relaxed);
    if (!FallbackReported_.exchange(true, std::memory_order_relaxed)) {
        Cerr << "warning: wasm UDF string column was materialized without a query"
                " compartment, falling back to a host string copy" << Endl;
    }
}

TPreferWasmStats::TSnapshot TPreferWasmStats::GetSnapshot() const {
    TSnapshot snapshot;
    snapshot.ColumnsMarked = ColumnsMarked_.load(std::memory_order_relaxed);
    snapshot.MaterializedInWasm = MaterializedInWasm_.load(std::memory_order_relaxed);
    snapshot.FallbackNoCompartment = FallbackNoCompartment_.load(std::memory_order_relaxed);
    snapshot.ResidentReused = ResidentReused_.load(std::memory_order_relaxed);
    snapshot.CopiedIntoCompartment = CopiedIntoCompartment_.load(std::memory_order_relaxed);
    return snapshot;
}

void TPreferWasmStats::Reset() {
    ColumnsMarked_.store(0, std::memory_order_relaxed);
    MaterializedInWasm_.store(0, std::memory_order_relaxed);
    FallbackNoCompartment_.store(0, std::memory_order_relaxed);
    ResidentReused_.store(0, std::memory_order_relaxed);
    CopiedIntoCompartment_.store(0, std::memory_order_relaxed);
    FallbackReported_.store(false, std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NKikimr::NUdfStore::NWasm
