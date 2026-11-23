#include "skeleton_burst_counter_publisher.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace {

class TBurstCounterPublisher final : public TActorBootstrapped<TBurstCounterPublisher> {
    const TIntrusivePtr<TVDiskContext> VCtx;
    const TDuration Interval;

public:
    TBurstCounterPublisher(TIntrusivePtr<TVDiskContext> vctx, TDuration interval)
        : VCtx(std::move(vctx))
        , Interval(interval)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Publish();
    }

private:
    void Publish() {
        VCtx->CostTracker->PublishBurstCounters();
        Schedule(Interval, new TEvents::TEvWakeup);
    }

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TSystem::Wakeup, Publish);
        cFunc(TEvents::TSystem::Poison, PassAway);
    )
};

} // namespace

IActor* CreateBurstCounterPublisher(TIntrusivePtr<TVDiskContext> vctx, TDuration interval) {
    return new TBurstCounterPublisher(std::move(vctx), interval);
}

} // namespace NKikimr
