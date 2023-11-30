#include "walle.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NCms {

using namespace NKikimrCms;
using namespace NNodeWhiteboard;

class TWalleRemoveTaskAdapter : public TActorBootstrapped<TWalleRemoveTaskAdapter> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_WALLE_REQ;
    }

    TWalleRemoveTaskAdapter(TEvCms::TEvWalleRemoveTaskRequest::TPtr &event, const TCmsStatePtr state, TActorId cms)
        : RequestEvent(event)
        , State(state)
        , Cms(cms)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        TAutoPtr<TEvCms::TEvWalleRemoveTaskResponse> response = new TEvCms::TEvWalleRemoveTaskResponse;
        TString id = RequestEvent->Get()->Record.GetTaskId();

        LOG_INFO(ctx, NKikimrServices::CMS, "Processing Wall-E request: %s",
                  RequestEvent->Get()->Record.ShortDebugString().data());

        if (!State->WalleTasks.contains(id)) {
            ReplyWithErrorAndDie(TStatus::WRONG_REQUEST, "Unknown task", ctx);
            return;
        }

        TAutoPtr<TEvCms::TEvRemoveWalleTask> event = new TEvCms::TEvRemoveWalleTask;
        event->TaskId = id;
        ctx.Send(Cms, event.Release());

        Become(&TThis::StateWork, ctx, TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, Timeout);
            CFunc(TEvCms::EvWalleTaskRemoved, Finish);
        default:
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS,
                      "TWalleRemoveTaskAdapter::StateWork ignored event type: %" PRIx32 " event: %s",
                      ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void ReplyWithErrorAndDie(TStatus::ECode code, const TString &err, const TActorContext &ctx) {
        TAutoPtr<TEvCms::TEvWalleRemoveTaskResponse> resp = new TEvCms::TEvWalleRemoveTaskResponse;
        resp->Record.MutableStatus()->SetCode(code);
        resp->Record.MutableStatus()->SetReason(err);
        ReplyAndDie(resp.Release(), ctx);
    }

    void ReplyAndDie(TAutoPtr<TEvCms::TEvWalleRemoveTaskResponse> resp, const TActorContext &ctx) {
        WalleAuditLog(RequestEvent->Get(), resp.Get(), ctx);
        ctx.Send(RequestEvent->Sender, resp.Release());
        Die(ctx);
    }

    void Timeout(const TActorContext &ctx) {
        ReplyWithErrorAndDie(TStatus::ERROR_TEMP, "Timeout", ctx);
    }

    void Finish(const TActorContext &ctx) {
        TAutoPtr<TEvCms::TEvWalleRemoveTaskResponse> resp = new TEvCms::TEvWalleRemoveTaskResponse;
        resp->Record.MutableStatus()->SetCode(TStatus::OK);
        ReplyAndDie(resp, ctx);
    }

    TEvCms::TEvWalleRemoveTaskRequest::TPtr RequestEvent;
    const TCmsStatePtr State;
    TActorId Cms;
};

IActor *CreateWalleAdapter(TEvCms::TEvWalleRemoveTaskRequest::TPtr &ev, const TCmsStatePtr state, TActorId cms) {
    return new TWalleRemoveTaskAdapter(ev, state, cms);
}

} // NKikimr::NCms
