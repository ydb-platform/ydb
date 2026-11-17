#include "checksum_admission.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NController {
namespace {

class TChecksumAdmission: public TActorBootstrapped<TChecksumAdmission> {
    using TEvAdmission = NSchemeShard::TEvSchemeShard::TEvChecksumAdmission;
    using TEvResult = NSchemeShard::TEvSchemeShard::TEvChecksumAdmissionResult;
public:
    TChecksumAdmission(TActorId parent, NKikimrReplication::TEvChecksumAdmission request)
        : Parent(parent)
        , Request(std::move(request))
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Retry();
    }

private:
    void SendRequest() {
        if (!Pipe) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {.RetryLimitCount = 3};
            Pipe = Register(NTabletPipe::CreateClient(SelfId(), Request.GetDestinationPathId().GetOwnerId(), config));
        }
        auto event = MakeHolder<TEvAdmission>();
        event->Record = Request;
        NTabletPipe::SendData(SelfId(), Pipe, event.Release());
    }

    void Retry() {
        SendRequest();
        Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->ClientId == Pipe && ev->Get()->Status != NKikimrProto::OK) {
            NTabletPipe::CloseAndForgetClient(SelfId(), Pipe);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId == Pipe) {
            Pipe = {};
        }
    }

    void Handle(TEvResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetRequestId() != Request.GetRequestId()) {
            return;
        }
        if (Request.GetRelease() && !record.GetReleased()) {
            return;
        }
        auto reply = MakeHolder<TEvResult>();
        reply->Record = record;
        Send(Parent, reply.Release());
        if (record.GetReleased()) {
            NTabletPipe::CloseAndForgetClient(SelfId(), Pipe);
            PassAway();
        }
    }

    void Release() {
        Request.SetRelease(true);
        SendRequest();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTabletPipe::TEvClientConnected, Handle)
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle)
        hFunc(TEvResult, Handle)
        sFunc(TEvents::TEvWakeup, Retry)
        sFunc(TEvents::TEvPoison, Release)
    )

    const TActorId Parent;
    NKikimrReplication::TEvChecksumAdmission Request;
    TActorId Pipe;
};

}

IActor* CreateChecksumAdmission(TActorId parent, NKikimrReplication::TEvChecksumAdmission request) {
    return new TChecksumAdmission(parent, std::move(request));
}

} // namespace NKikimr::NReplication::NController
