#include "common.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

void TCPUUsage::Exchange(const TDuration predicted, const TMonotonic start, const TMonotonic finish) {
    {
        TTaskCPUUsage usage(start, finish);
        if (usage.Cut(StartInstant)) {
            Usage.emplace_back(TTaskCPUUsage(start, finish));
            Duration += Usage.back().GetDuration();
        }
    }
    AFL_VERIFY(predicted <= PredictedDuration)("predicted_delta", predicted)("predicted_sum", PredictedDuration);
    PredictedDuration -= predicted;
    if (Parent) {
        Parent->Exchange(predicted, start, finish);
    }
}

void TCPUUsage::Cut(const TMonotonic start) {
    StartInstant = start;
    ui32 idx = 0;
    while (idx < Usage.size()) {
        AFL_VERIFY(Usage[idx].GetDuration() <= Duration);
        Duration -= Usage[idx].GetDuration();
        if (Usage[idx].GetFinish() <= start) {
            std::swap(Usage[idx], Usage.back());
            Usage.pop_back();
        } else {
            AFL_VERIFY(Usage[idx].Cut(start));
            Duration += Usage[idx].GetDuration();
            ++idx;
        }
    }
}

}   // namespace NKikimr::NConveyorComposite
