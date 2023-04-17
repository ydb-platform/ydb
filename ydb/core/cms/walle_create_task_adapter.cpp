#include "cms_impl.h"
#include "info_collector.h"
#include "walle.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NCms {

using namespace NKikimrCms;

class TWalleCreateTaskAdapter : public TActorBootstrapped<TWalleCreateTaskAdapter> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_WALLE_REQ;
    }

    TWalleCreateTaskAdapter(TEvCms::TEvWalleCreateTaskRequest::TPtr &event, TActorId cms)
        : RequestEvent(event)
        , Cms(cms)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        auto &rec = RequestEvent->Get()->Record;

        LOG_INFO(ctx, NKikimrServices::CMS, "Processing Wall-E request: %s",
                  rec.ShortDebugString().data());

        if (rec.GetAction() != "reboot"
            && rec.GetAction() != "power-off"
            && rec.GetAction() != "change-disk"
            && rec.GetAction() != "change-memory"
            && rec.GetAction() != "profile"
            && rec.GetAction() != "redeploy"
            && rec.GetAction() != "prepare"
            && rec.GetAction() != "repair-link"
            && rec.GetAction() != "repair-bmc"
            && rec.GetAction() != "repair-overheat"
            && rec.GetAction() != "repair-capping"
            && rec.GetAction() != "deactivate"
            && rec.GetAction() != "temporary-unreachable") {
            ReplyWithErrorAndDie(TStatus::WRONG_REQUEST, "Unsupported action", ctx);
            return;
        }

        if (!rec.HostsSize()) {
            ReplyWithErrorAndDie(TStatus::WRONG_REQUEST, "No hosts specified", ctx);
            return;
        }

        ctx.Send(Cms, new TEvCms::TEvGetClusterInfoRequest);

        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCms::TEvPermissionResponse, Handle);
            HFunc(TEvCms::TEvGetClusterInfoResponse, Handle);
            CFunc(TEvCms::EvWalleTaskStored, Finish);
            HFunc(TEvCms::TEvStoreWalleTaskFailed, Handle);
        default:
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS,
                      "TWalleCreateTaskAdapter::StateWork ignored event type: %" PRIx32 " event: %s",
                      ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void ReplyWithErrorAndDie(TStatus::ECode code, const TString &err, const TActorContext &ctx) {
        auto &rec = RequestEvent->Get()->Record;
        TAutoPtr<TEvCms::TEvWalleCreateTaskResponse> resp = new TEvCms::TEvWalleCreateTaskResponse;
        resp->Record.MutableStatus()->SetCode(code);
        resp->Record.MutableStatus()->SetReason(err);
        resp->Record.SetTaskId(rec.GetTaskId());
        resp->Record.MutableHosts()->CopyFrom(rec.GetHosts());
        ReplyAndDie(resp.Release(), ctx);
    }

    void ReplyAndDie(TAutoPtr<TEvCms::TEvWalleCreateTaskResponse> resp, const TActorContext &ctx) {
        WalleAuditLog(RequestEvent->Get(), resp.Get(), ctx);
        ctx.Send(RequestEvent->Sender, resp.Release());
        Die(ctx);
    }

    void Handle(TEvCms::TEvPermissionResponse::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;

        Response = new TEvCms::TEvWalleCreateTaskResponse;
        Response->Record.MutableStatus()->CopyFrom(rec.GetStatus());
        Response->Record.SetTaskId(RequestEvent->Get()->Record.GetTaskId());
        Response->Record.MutableHosts()->CopyFrom(RequestEvent->Get()->Record.GetHosts());

        // In case of success or scheduled request we have to store
        // task information.
        if ((rec.GetStatus().GetCode() == TStatus::ALLOW
             || rec.GetStatus().GetCode() == TStatus::DISALLOW_TEMP)
            && !RequestEvent->Get()->Record.GetDryRun()) {
            TAutoPtr<TEvCms::TEvStoreWalleTask> event = new TEvCms::TEvStoreWalleTask;
            event->Task.TaskId = RequestEvent->Get()->Record.GetTaskId();
            event->Task.RequestId = rec.GetRequestId();

            for (auto &permission : rec.GetPermissions())
                event->Task.Permissions.insert(permission.GetId());

            ctx.Send(Cms, event.Release());
            return;
        }

        ReplyAndDie(Response, ctx);
    }

    void Handle(TEvCms::TEvGetClusterInfoResponse::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->Info->IsOutdated()) {
            ReplyWithErrorAndDie(TStatus::ERROR_TEMP, "Cannot collect cluster info", ctx);
            return;
        }

        auto cluster = ev->Get()->Info;
        auto &task = RequestEvent->Get()->Record;

        for (auto &host : task.GetHosts()) {
            if (!cluster->HasNode(host)) {
                ReplyWithErrorAndDie(TStatus::WRONG_REQUEST, "Unknown host " + host, ctx);
                return;
            }
        }

        TAutoPtr<TEvCms::TEvPermissionRequest> request = new TEvCms::TEvPermissionRequest;
        request->Record.SetUser(WALLE_CMS_USER);
        request->Record.SetSchedule(true);
        request->Record.SetDryRun(task.GetDryRun());

        TAction action;
        if (task.GetAction() == "prepare"
            || task.GetAction() == "deactivate") {
            TAutoPtr<TEvCms::TEvWalleCreateTaskResponse> resp = new TEvCms::TEvWalleCreateTaskResponse;
            resp->Record.SetTaskId(task.GetTaskId());
            resp->Record.MutableHosts()->CopyFrom(task.GetHosts());
            resp->Record.MutableStatus()->SetCode(TStatus::OK);
            ReplyAndDie(resp.Release(), ctx);
            return;
        } else {
            // We always use infinite duration.
            // Wall-E MUST delete processed tasks.
            if (task.GetAction() == "reboot") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "power-off") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "change-disk") {
                action.SetType(TAction::REPLACE_DEVICES);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "change-memory") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "profile") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "redeploy") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "repair-link") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "repair-bmc") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "repair-overheat") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "repair-capping") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else if (task.GetAction() == "temporary-unreachable") {
                action.SetType(TAction::SHUTDOWN_HOST);
                action.SetDuration(TDuration::Max().GetValue());
            } else
                Y_FAIL("Unknown action");

            for (auto &host : task.GetHosts()) {
                auto &hostAction = *request->Record.AddActions();
                hostAction.CopyFrom(action);
                hostAction.SetHost(host);
                if (action.GetType() == TAction::REPLACE_DEVICES) {
                    for (const auto node : cluster->HostNodes(host)) {
                        for (auto &pdiskId : node->PDisks)
                            *hostAction.AddDevices() = cluster->PDisk(pdiskId).GetDeviceName();
                    }
                }
            }
        }

        ctx.Send(Cms, request.Release());
    }

    void Handle(TEvCms::TEvStoreWalleTaskFailed::TPtr &ev, const TActorContext &ctx) {
        ReplyWithErrorAndDie(TStatus::ERROR_TEMP, ev.Get()->Get()->Reason, ctx);
    }

    void Finish(const TActorContext &ctx) {
        ReplyAndDie(Response, ctx);
    }

    TEvCms::TEvWalleCreateTaskRequest::TPtr RequestEvent;
    TAutoPtr<TEvCms::TEvWalleCreateTaskResponse> Response;
    TActorId Cms;
};

IActor *CreateWalleAdapter(TEvCms::TEvWalleCreateTaskRequest::TPtr &ev, TActorId cms) {
    return new TWalleCreateTaskAdapter(ev, cms);
}

} // namespace NKikimr::NCms
