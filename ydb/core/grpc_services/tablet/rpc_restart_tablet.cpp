#include "rpc_restart_tablet.h"
#include "service_tablet.h"

#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NGRpcService {

class TRpcRestartTablet : public TRpcRequestActor<TRpcRestartTablet, TEvRestartTabletRequest> {
    using TBase = TRpcRequestActor<TRpcRestartTablet, TEvRestartTabletRequest>;

public:
    using TBase::TBase;

    void Bootstrap() {
        if (!CheckAccess()) {
            auto error = TStringBuilder() << "Access denied";
            if (this->UserToken) {
                error << ": '" << this->UserToken->GetUserSID() << "' is not an admin";
            }

            this->Reply(Ydb::StatusIds::UNAUTHORIZED, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return;
        }

        auto* req = this->GetProtoRequest();
        TabletId = req->tablet_id();
        PipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), TabletId, NTabletPipe::TClientRetryPolicy{
            // We need at least one retry since local resolver cache may be outdated
            .RetryLimitCount = 1,
        }));

        Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup);

        Become(&TThis::StateWork);
    }

private:
    bool CheckAccess() const {
        if (AppData()->AdministrationAllowedSIDs.empty()) {
            return true;
        }

        if (!this->UserToken) {
            return false;
        }

        for (const auto& sid : AppData()->AdministrationAllowedSIDs) {
            if (this->UserToken->IsExist(sid)) {
                return true;
            }
        }

        return false;
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        auto* msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            this->Reply(Ydb::StatusIds::UNAVAILABLE,
                TStringBuilder() << "Tablet " << TabletId << " is unavailable");
            return;
        }

        NTabletPipe::SendData(SelfId(), PipeClient, new TEvents::TEvPoison);
        NTabletPipe::CloseClient(SelfId(), PipeClient);
        this->Reply(Ydb::StatusIds::SUCCESS);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        this->Reply(Ydb::StatusIds::UNDETERMINED,
            TStringBuilder() << "Tablet " << TabletId << " disconnected");
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        NTabletPipe::CloseClient(SelfId(), PipeClient);
        this->Reply(Ydb::StatusIds::TIMEOUT,
            TStringBuilder() << "Tablet " << TabletId << " is not responding");
    }

private:
    ui64 TabletId;
    TActorId PipeClient;
};

void DoRestartTabletRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRpcRestartTablet(p.release()));
}

template<>
IActor* TEvRestartTabletRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestNoOpCtx* msg) {
    return new TRpcRestartTablet(msg);
}

} // namespace NKikimr::NGRpcService
