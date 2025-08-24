#include "cms_impl.h"
#include "info_collector.h"
#include "walle.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <optional>

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

        if (!Actions.contains(rec.GetAction())) {
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
        resp->Record.MutableDevices()->CopyFrom(rec.GetDevices());
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
        Response->Record.MutableDevices()->CopyFrom(RequestEvent->Get()->Record.GetDevices());

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

    void HandleReplaceDevicesAction(const TClusterInfoPtr &cluster, 
                                    TAutoPtr<TEvCms::TEvPermissionRequest> &request, 
                                    ui64 duration, 
                                    const TActorContext &ctx) {
        auto &task = RequestEvent->Get()->Record;

        if (task.DevicesSize() > 0) {
            // Replace specified devices on the specified host.
            if (task.HostsSize() != 1) {
                ReplyWithErrorAndDie(TStatus::WRONG_REQUEST, "Exactly one host must be specified if \"devices\" are set", ctx);
                return;
            }
            TString host = task.get_idx_hosts(0);
            auto &action = *request->Record.AddActions();
            action.SetHost(host);
            action.SetType(TAction::REPLACE_DEVICES);
            action.SetDuration(duration);

            for (const auto& device : task.GetDevices())
                *action.AddDevices() = device;
        } else {
            // Replace all devices on all provided hosts.
            for (auto &host : task.GetHosts()) {
                auto &action = *request->Record.AddActions();
                action.SetHost(host);
                action.SetType(TAction::REPLACE_DEVICES);
                action.SetDuration(duration);
                for (const auto node : cluster->HostNodes(host)) {
                    for (auto &pdiskId : node->PDisks)
                        *action.AddDevices() = cluster->PDisk(pdiskId).GetDeviceName();
                }
            }
        }
    }

    void HandleGenericAction(TAutoPtr<TEvCms::TEvPermissionRequest> &cmsRequest, TAction::EType actionType, ui64 duration) {
        auto &task = RequestEvent->Get()->Record;

        for (auto &host : task.GetHosts()) {
            auto &action = *cmsRequest->Record.AddActions();
            action.SetHost(host);
            action.SetType(actionType);
            action.SetDuration(duration);
        }
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
        const auto &action = task.GetAction();
        if (action == "temporary-unreachable") {
            request->Record.SetPriority(WALLE_SOFT_MAINTENANCE_PRIORITY);
        } else {
            request->Record.SetPriority(WALLE_DEFAULT_PRIORITY);
        }
        
        auto it = Actions.find(action);
        Y_ABORT_UNLESS(it != Actions.end());

        if (!it->second) {
            TAutoPtr<TEvCms::TEvWalleCreateTaskResponse> resp = new TEvCms::TEvWalleCreateTaskResponse;
            resp->Record.SetTaskId(task.GetTaskId());
            resp->Record.MutableHosts()->CopyFrom(task.GetHosts());
            resp->Record.MutableDevices()->CopyFrom(task.GetDevices());
            resp->Record.MutableStatus()->SetCode(TStatus::OK);
            ReplyAndDie(resp.Release(), ctx);
            return;
        } else {
            // We always use infinite duration.
            // Wall-E MUST delete processed tasks.
            ui64 duration = TDuration::Max().GetValue();

            TAction::EType actionType = *it->second;
            if (actionType == TAction::REPLACE_DEVICES) {
                HandleReplaceDevicesAction(cluster, request, duration, ctx);
            } else {
                HandleGenericAction(request, actionType, duration);
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

    static const THashMap<TString, std::optional<TAction::EType>> Actions;
    TEvCms::TEvWalleCreateTaskRequest::TPtr RequestEvent;
    TAutoPtr<TEvCms::TEvWalleCreateTaskResponse> Response;
    TActorId Cms;
};

const THashMap<TString, std::optional<TAction::EType>> TWalleCreateTaskAdapter::Actions = {
    {"reboot", TAction::REBOOT_HOST},
    {"power-off", TAction::SHUTDOWN_HOST},
    {"change-disk", TAction::REPLACE_DEVICES},
    {"change-memory", TAction::SHUTDOWN_HOST},
    {"profile", TAction::SHUTDOWN_HOST},
    {"redeploy", TAction::SHUTDOWN_HOST},
    {"repair-link", TAction::SHUTDOWN_HOST},
    {"repair-bmc", TAction::SHUTDOWN_HOST},
    {"repair-overheat", TAction::SHUTDOWN_HOST},
    {"repair-capping", TAction::SHUTDOWN_HOST},
    {"temporary-unreachable", TAction::SHUTDOWN_HOST},
    {"prepare", std::nullopt},
    {"deactivate", std::nullopt},
};

IActor *CreateWalleAdapter(TEvCms::TEvWalleCreateTaskRequest::TPtr &ev, TActorId cms) {
    return new TWalleCreateTaskAdapter(ev, cms);
}

} // namespace NKikimr::NCms
