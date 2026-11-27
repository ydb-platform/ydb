#pragma once
#include <util/generic/hash_set.h>

#include <optional>

namespace NKikimr::NColumnShard {

// Any coordinator's NextReadStep is at least the mediator step the shard last observed before restart.
class TMediatorTimeCatchUp {
private:
    THashSet<ui64> Coordinators;
    THashSet<ui64> HasReplied;
    std::optional<ui64> TargetStep;

public:
    enum class EStatus {
        WaitingForCoordinator,
        WaitingForStep,
        CaughtUp
    };

    struct TCheckResult {
        EStatus Status;
        ui64 TargetStep = 0;
    };

    explicit TMediatorTimeCatchUp(const THashSet<ui64>& coordinators);

    void OnNextReadStep(const ui64 coordinatorId, const ui64 nextReadStep);

    TCheckResult Check(ui64 mediatorStep) const;
};

}   // namespace NKikimr::NColumnShard
