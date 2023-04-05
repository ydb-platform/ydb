#include "walle.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NCms {

using namespace NKikimrCms;
using namespace NNodeWhiteboard;

class TWalleCheckTaskAdapter : public TActorBootstrapped<TWalleCheckTaskAdapter> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_WALLE_REQ;
    }

    TWalleCheckTaskAdapter(TEvCms::TEvWalleCheckTaskRequest::TPtr &event,
                           const TCmsStatePtr state, TActorId cms)
        : RequestEvent(event)
        , State(state)
        , Cms(cms)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        TString id = RequestEvent->Get()->Record.GetTaskId();

        LOG_INFO(ctx, NKikimrServices::CMS, "Processing Wall-E request: %s",
                  RequestEvent->Get()->Record.ShortDebugString().data());

        if (!State->WalleTasks.contains(id)) {
            ReplyWithErrorAndDie(TStatus::WRONG_REQUEST, "Unknown task", ctx);
            return;
        }

        Response = new TEvCms::TEvWalleCheckTaskResponse;
        auto &info = *Response->Record.MutableTask();
        auto &task = State->WalleTasks.find(id)->second;
        info.SetTaskId(id);

        if (State->ScheduledRequests.contains(task.RequestId)) {
            auto &req = State->ScheduledRequests.find(task.RequestId)->second;

            for (auto &action : req.Request.GetActions())
                *info.AddHosts() = action.GetHost();

            TAutoPtr<TEvCms::TEvCheckRequest> event = new TEvCms::TEvCheckRequest;
            event->Record.SetUser(WALLE_CMS_USER);
            event->Record.SetRequestId(task.RequestId);

            ctx.Send(Cms, event.Release());

            Become(&TThis::StateWork, ctx, TDuration::Seconds(10), new TEvents::TEvWakeup());
        } else {
            for (auto &id : task.Permissions) {
                if (State->Permissions.contains(id))
                    *info.AddHosts() = State->Permissions.find(id)->second.Action.GetHost();
            }

            if (!info.HostsSize()) {
                ReplyWithErrorAndDie(TStatus::WRONG_REQUEST, "Empty task", ctx);
                return;
            }

            Response->Record.MutableStatus()->SetCode(TStatus::ALLOW);

            ReplyAndDie(Response, ctx);
        }
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCms::TEvPermissionResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        default:
            LOG_DEBUG(ctx, NKikimrServices::CMS,
                      "TWalleRemoveTaskAdapter::StateWork ignored event type: %" PRIx32 " event: %s",
                      ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void ReplyWithErrorAndDie(TStatus::ECode code, const TString &err, const TActorContext &ctx) {
        TAutoPtr<TEvCms::TEvWalleCheckTaskResponse> resp = new TEvCms::TEvWalleCheckTaskResponse;
        resp->Record.MutableStatus()->SetCode(code);
        resp->Record.MutableStatus()->SetReason(err);
        ReplyAndDie(resp.Release(), ctx);
    }

    void ReplyAndDie(TAutoPtr<TEvCms::TEvWalleCheckTaskResponse> resp, const TActorContext &ctx) {
        WalleAuditLog(RequestEvent->Get(), resp.Get(), ctx);
        ctx.Send(RequestEvent->Sender, resp.Release());
        Die(ctx);
    }

    void Handle(TEvCms::TEvPermissionResponse::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;

        Response->Record.MutableStatus()->CopyFrom(rec.GetStatus());
        ReplyAndDie(Response, ctx);
    }

    void Timeout(const TActorContext &ctx) {
        ReplyWithErrorAndDie(TStatus::ERROR_TEMP, "Timeout", ctx);
    }

    TEvCms::TEvWalleCheckTaskRequest::TPtr RequestEvent;
    TAutoPtr<TEvCms::TEvWalleCheckTaskResponse> Response;
    const TCmsStatePtr State;
    TActorId Cms;
};

IActor *CreateWalleAdapter(TEvCms::TEvWalleCheckTaskRequest::TPtr &ev, const TCmsStatePtr state, TActorId cms) {
    return new TWalleCheckTaskAdapter(ev, state, cms);
}

} // namespace NKikimr::NCms
