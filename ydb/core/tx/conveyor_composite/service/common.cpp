#include "common.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

void TCPUUsage::Exchange(const TDuration predicted, const TMonotonic start, const TMonotonic finish) {
    Usage.emplace_back(TTaskCPUUsage(start, finish));
    AFL_VERIFY(predicted <= PredictedDuration)("predicted_delta", predicted)("predicted_sum", PredictedDuration);
    Duration += Usage.back().GetDuration();
    PredictedDuration -= predicted;
    if (Parent) {
        Parent->Exchange(predicted, start, finish);
    }
}

void TCPUUsage::Cut(const TMonotonic start) {
    ui32 idx = 0;
    while (idx < Usage.size()) {
        AFL_VERIFY(Usage[idx].GetDuration() <= Duration);
        Duration -= Usage[idx].GetDuration();
        if (Usage[idx].GetFinish() <= start) {
            std::swap(Usage[idx], Usage.back());
            Usage.pop_back();
        } else {
            Usage[idx].Cut(start);
            Duration += Usage[idx].GetDuration();
            ++idx;
        }
    }
}

TCPUGroup::~TCPUGroup() {
    AFL_VERIFY(ProcessesCount == 0);
}

}   // namespace NKikimr::NConveyorComposite
