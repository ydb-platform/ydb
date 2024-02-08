#include "flat_executor_bootlogic.h"
#include <ydb/core/util/pb.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

class TLeaseWaiter : public TActorBootstrapped<TLeaseWaiter> {
public:
    TLeaseWaiter(
            const TActorId& owner,
            TMonotonic bootTimestamp,
            const TActorId& leaseHolder,
            TDuration leaseDuration)
        : Owner(owner)
        , BootTimestamp(bootTimestamp)
        , LeaseHolder(leaseHolder)
        , LeaseDuration(leaseDuration)
    { }

    void Bootstrap() {
        SendRequest();
        Schedule(BootTimestamp + LeaseDuration * 2, new TEvents::TEvWakeup);
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoison, PassAway);
            sFunc(TEvents::TEvWakeup, HandleTimeout);
            hFunc(TEvTablet::TEvLeaseDropped, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
        }
    }

    void SendRequest() {
        Send(LeaseHolder, new TEvTablet::TEvDropLease(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
    }

    void Finish() {
        Send(Owner, new TEvTablet::TEvLeaseDropped());
        PassAway();
    }

    void HandleTimeout() {
        Finish();
    }

    void Handle(TEvTablet::TEvLeaseDropped::TPtr&) {
        Finish();
    }

    void Handle(TEvInterconnect::TEvNodeConnected::TPtr&) {
        // We don't need to do anything
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        // Keep retrying until timeout is reached
        SendRequest();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        auto* msg = ev->Get();
        Y_ABORT_UNLESS(ev->Sender == LeaseHolder);
        Y_ABORT_UNLESS(msg->SourceType == TEvTablet::TEvDropLease::EventType);
        if (msg->Reason == TEvents::TEvUndelivered::ReasonActorUnknown) {
            // We have proved lease holder no longer exists
            return Finish();
        }

        // We may get undelivered notifications due to disconnections
        // Since we cannot trust them we expect to retry on TEvNodeDisconnected
    }

private:
    const TActorId Owner;
    const TMonotonic BootTimestamp;
    const TActorId LeaseHolder;
    const TDuration LeaseDuration;
};

void TExecutorBootLogic::StartLeaseWaiter(TMonotonic bootTimestamp, const TEvTablet::TDependencyGraph& graph) noexcept
{
    TActorId leaseHolder;
    TDuration leaseDuration;

    for (const auto& entry : graph.Entries) {
        for (const auto& meta : entry.EmbeddedMetadata) {
            if (NBoot::ELogCommitMeta(meta.Key) == NBoot::ELogCommitMeta::LeaseInfo) {
                TProtoBox<NKikimrExecutorFlat::TLeaseInfoMetadata> proto(meta.Data);
                if (proto.HasLeaseHolder()) {
                    leaseHolder = ActorIdFromProto(proto.GetLeaseHolder());
                }
                if (proto.HasLeaseDurationUs()) {
                    leaseDuration = TDuration::MicroSeconds(proto.GetLeaseDurationUs());
                }
            }
        }
    }

    if (leaseHolder && leaseDuration) {
        LeaseWaiter = Ops->RegisterWithSameMailbox(new TLeaseWaiter(SelfId, bootTimestamp, leaseHolder, leaseDuration));
    }
}

} // namespace NTabletFlatExecutor
} // namespace NKikimr
