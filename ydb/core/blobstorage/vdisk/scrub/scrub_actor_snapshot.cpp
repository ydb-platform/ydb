#include "scrub_actor_impl.h"

namespace NKikimr {

    void TScrubCoroImpl::TakeSnapshot() {
        Send(ScrubCtx->SkeletonId, new TEvTakeHullSnapshot(false));
        CurrentState = TStringBuilder() << "waiting for Hull snapshot";
        auto res = WaitForSpecificEvent<TEvTakeHullSnapshotResult>(&TScrubCoroImpl::ProcessUnexpectedEvent);
        Snap.emplace(std::move(res->Get()->Snap));
        Snap->BlocksSnap.Destroy(); // blocks are not needed for operation
    }

    void TScrubCoroImpl::ReleaseSnapshot() {
        Snap.reset();
        BarriersEssence.Reset();
    }

    TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> TScrubCoroImpl::GetBarriersEssence() {
        if (!BarriersEssence) {
            BarriersEssence = Snap->BarriersSnap.CreateEssence(Snap->HullCtx);
            Snap->BarriersSnap.Destroy();
        }
        return BarriersEssence;
    }

} // NKikimr
