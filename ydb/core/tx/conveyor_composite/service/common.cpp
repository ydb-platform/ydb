#include "common.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NConveyorComposite {

void TCPUUsage::Exchange(const TDuration predicted, const TMonotonic start, const TMonotonic finish) {
    Duration += finish - start;
    AFL_VERIFY(predicted <= PredictedDuration)("predicted_delta", predicted)("predicted_sum", PredictedDuration);
    PredictedDuration -= predicted;
    if (Parent) {
        Parent->Exchange(predicted, start, finish);
    }
}

}   // namespace NKikimr::NConveyorComposite
