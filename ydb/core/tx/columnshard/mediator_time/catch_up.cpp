#include "catch_up.h"

#include <ydb/library/actors/core/log.h>

#include <algorithm>

namespace NKikimr::NColumnShard {

TMediatorTimeCatchUp::TMediatorTimeCatchUp(const THashSet<ui64>& coordinators)
    : Coordinators(coordinators)
{
    if (Coordinators.empty()) {
        TargetStep = 0;
    }
}

void TMediatorTimeCatchUp::OnNextReadStep(const ui64 coordinatorId, const ui64 nextReadStep) {
    YDB_LOG_VERIFY(Coordinators.contains(coordinatorId), {"coordinatorId", coordinatorId});
    if (!HasReplied.insert(coordinatorId).second) {
        return;
    }
    TargetStep = TargetStep ? std::min(*TargetStep, nextReadStep) : nextReadStep;
}

TMediatorTimeCatchUp::TCheckResult TMediatorTimeCatchUp::Check(ui64 mediatorStep) const {
    if (!TargetStep) {
        return { EStatus::WaitingForCoordinator };
    }
    if (mediatorStep < *TargetStep) {
        return { EStatus::WaitingForStep, *TargetStep };
    }
    return { EStatus::CaughtUp, *TargetStep };
}

}   // namespace NKikimr::NColumnShard
