#include "client.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NIamDelegation {
namespace {

class TTabletRequest final : public NActors::TActorBootstrapped<TTabletRequest> {
public:
    explicit TTabletRequest(ui64 tabletId)
        : TabletId(tabletId)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Schedule(TDuration::Seconds(30), new NActors::TEvents::TEvWakeup);
    }

private:
    void Handle(TEvIamDelegationTablet::TEvRequest::TPtr& ev) {
        if (Requester) {
            return;
        }
        Requester = ev->Sender;
        Cookie = ev->Cookie;
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        Pipe = Register(NTabletPipe::CreateClient(SelfId(), TabletId, config));
        NTabletPipe::SendData(SelfId(), Pipe, ev->Release().Release());
    }

    void Handle(TEvIamDelegationTablet::TEvResponse::TPtr& ev) {
        Send(Requester, ev->Release().Release(), 0, Cookie);
        PassAway();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Fail("Cannot connect to IAM delegation tablet");
        }
    }

    void Fail(const TString& error) {
        if (Requester) {
            auto reply = MakeHolder<TEvIamDelegationTablet::TEvResponse>();
            reply->Record.SetStatus(NKikimrIamDelegation::STATUS_UNSPECIFIED);
            reply->Record.SetError(error);
            Send(Requester, reply.Release(), 0, Cookie);
        }
        PassAway();
    }

    void Disconnected() { Fail("IAM delegation tablet connection was lost; request outcome is unknown"); }
    void TimedOut() { Fail("IAM delegation tablet request timed out; request outcome is unknown"); }

    void PassAway() override {
        if (Pipe) {
            NTabletPipe::CloseClient(SelfId(), Pipe);
        }
        TActorBootstrapped::PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegationTablet::TEvRequest, Handle);
        hFunc(TEvIamDelegationTablet::TEvResponse, Handle);
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        cFunc(TEvTabletPipe::TEvClientDestroyed::EventType, Disconnected);
        cFunc(NActors::TEvents::TEvWakeup::EventType, TimedOut);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

    const ui64 TabletId;
    NActors::TActorId Requester;
    NActors::TActorId Pipe;
    ui64 Cookie = 0;
};

} // namespace

NActors::IActor* CreateIamDelegationTabletRequestActor(ui64 tabletId) {
    return new TTabletRequest(tabletId);
}

} // namespace NKikimr::NIamDelegation
