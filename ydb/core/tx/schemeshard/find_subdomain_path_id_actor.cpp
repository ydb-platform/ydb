#include "schemeshard.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/random/random.h>

namespace NKikimr {
namespace NSchemeShard {

/**
 * It is client for SchemeShard TEvFindTabletSubDomainPathId event.
 *
 * Allows you to time-distribute a large number of requests from Topics and DataShared actors during a cluster reboot.
 */
class TFindSubDomainPathIdActor : public TActorBootstrapped<TFindSubDomainPathIdActor> {
    using TBase = TActorBootstrapped<TFindSubDomainPathIdActor>;

public:
    TFindSubDomainPathIdActor(const TActorId& parent, ui64 tabletId, ui64 schemeShardId, bool delayFirstRequest, TDuration maxFindSubDomainPathIdDelay)
        : Parent(parent)
        , TabletId(tabletId)
        , SchemeShardId(schemeShardId)
        , DelayNextRequest(delayFirstRequest)
        , MaxFindSubDomainPathIdDelay(maxFindSubDomainPathIdDelay)
    { }

    void Bootstrap() {
        if (DelayNextRequest) {
            // Wait up to a large delay, so requests from shards spread over time
            auto delay = TDuration::MicroSeconds(RandomNumber(MaxFindSubDomainPathIdDelay.MicroSeconds()));
            Timer = CreateLongTimer(TActivationContext::AsActorContext(), delay,
                new IEventHandle(SelfId(), SelfId(), new TEvents::TEvWakeup));
            Become(&TThis::StateSleep);
        } else {
            DelayNextRequest = true;
            WakeUp();
        }
    }

    void PassAway() override {
        if (Timer) {
            Send(Timer, new TEvents::TEvPoison);
        }
        NTabletPipe::CloseAndForgetClient(SelfId(), SchemeShardPipe);
        TBase::PassAway();
    }

private:
    STFUNC(StateSleep) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoison, PassAway);
            sFunc(TEvents::TEvWakeup, WakeUp);
        }
    }

    void WakeUp() {
        Timer = { };
        SchemeShardPipe = Register(NTabletPipe::CreateClient(SelfId(), SchemeShardId));
        NTabletPipe::SendData(SelfId(), SchemeShardPipe,
            new NSchemeShard::TEvSchemeShard::TEvFindTabletSubDomainPathId(TabletId));
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoison, PassAway);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvFindTabletSubDomainPathIdResult, Handle);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->Status != NKikimrProto::OK) {
            // We could not connect to schemeshard, try again
            SchemeShardPipe = { };
            Bootstrap();
            return;
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->ClientId == SchemeShardPipe) {
            // We lost connection to schemeshard, try again
            SchemeShardPipe = { };
            Bootstrap();
            return;
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvFindTabletSubDomainPathIdResult::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->Record.GetStatus() != NKikimrScheme::TEvFindTabletSubDomainPathIdResult::SUCCESS) {
            // The request failed for some reason, we just stop trying in that case
            PassAway();
            return;
        }

        Send(Parent, new NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound(msg->Record.GetSchemeShardId(), msg->Record.GetSubDomainPathId()));
        PassAway();
    }

private:
    const TActorId Parent;
    const ui64 TabletId;
    const ui64 SchemeShardId;
    bool DelayNextRequest;
    TActorId Timer;
    TActorId SchemeShardPipe;
    TDuration MaxFindSubDomainPathIdDelay;
};


} // namespace NSchemeShard

IActor* CreateFindSubDomainPathIdActor(const TActorId& parent, ui64 tabletId, ui64 schemeShardId, bool delayFirstRequest, TDuration maxFindSubDomainPathIdDelay) {
    return new NSchemeShard::TFindSubDomainPathIdActor(parent, tabletId, schemeShardId, delayFirstRequest, maxFindSubDomainPathIdDelay);
}

} // namespace NKikimr
