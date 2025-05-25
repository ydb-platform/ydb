#include "scope.h"

namespace NKikimr::NConveyorComposite {

bool TProcessScope::HasTasks() const {
    for (auto&& i : Processes) {
        if (i.second.HasTasks()) {
            return true;
        }
    }
    return false;
}

TWorkerTask TProcessScope::ExtractTaskWithPrediction() {
    TProcess* pMin = nullptr;
    TDuration dMin = TDuration::Max();
    for (auto&& [_, p] : Processes) {
        if (!p.HasTasks()) {
            continue;
        }
        const TDuration d = p.GetCPUUsage()->CalcWeight(p.GetWeight());
        if (!pMin || d < dMin) {
            pMin = &p;
            dMin = d;
        }
    }
    AFL_VERIFY(pMin)("size", Processes.size());
    return pMin->ExtractTaskWithPrediction();
}

void TProcessScope::DoQuant(const TMonotonic newStart) {
    CPUUsage->Cut(newStart);
    for (auto&& i : Processes) {
        i.second.DoQuant(newStart);
    }
}

}
