#include "abstract.h"

namespace NKikimr::NOlap {

    void TNormalizationController::RegisterNormalizer(INormalizerComponent::TPtr normalizer) {
        Counters.emplace_back(normalizer->GetName());
        Normalizers.push_back(normalizer);
    }

    const INormalizerComponent::TPtr& TNormalizationController::GetNormalizer() const {
        Y_ABORT_UNLESS(CurrentNormalizerIndex < Normalizers.size());
        return Normalizers[CurrentNormalizerIndex];
    }

    const TNormalizerCounters& TNormalizationController::GetCounters() const {
        Y_ABORT_UNLESS(CurrentNormalizerIndex < Normalizers.size());
        return Counters[CurrentNormalizerIndex];
    }

    bool TNormalizationController::IsNormalizationFinished() const {
        return CurrentNormalizerIndex >= Normalizers.size();
    }

    bool TNormalizationController::SwitchNormalizer() {
        if (IsNormalizationFinished()) {
            return false;
        }
        Y_ABORT_UNLESS(!GetNormalizer()->WaitResult());
        GetCounters().OnNormalizerFinish();
        ++CurrentNormalizerIndex;
        return !IsNormalizationFinished();
    }
}
