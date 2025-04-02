#include "walle.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/cms/cms.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NCms {

using namespace NKikimrCms;

class TWalleCrateTaskHandler : public TActorBootstrapped<TWalleCrateTaskHandler> {
public:
    using TBase = TActorBootstrapped<TWalleCrateTaskHandler>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SERVICE_PROXY;
    }

    TWalleCrateTaskHandler(NMon::TEvHttpInfo::TPtr &event)
        : RequestEvent(event)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        NMon::TEvHttpInfo *msg = RequestEvent->Get();

        TString path {msg->Request.GetPathInfo().substr(strlen(WALLE_API_URL_PREFIX))};

        if (path == "tasks") {
            if (msg->Request.GetMethod() == HTTP_METHOD_GET)
                ProcessListTasksRequest(ctx);
            else if (msg->Request.GetMethod() == HTTP_METHOD_POST)
                ProcessCreateTasksRequest(msg->Request, ctx);
            else
                ReplyWithError("HTTP/1.1 405 Method Not Allowed\r\n\r\n", ctx);
        } else if (path.StartsWith("tasks/")) {
            TString id = path.substr(strlen("tasks/"));
            if (msg->Request.GetMethod() == HTTP_METHOD_GET)
                ProcessCheckTaskRequest(id, ctx);
            else if (msg->Request.GetMethod() == HTTP_METHOD_DELETE)
                ProcessRemoveTaskRequest(id, ctx);
            else
                ReplyWithError("HTTP/1.1 405 Method Not Allowed\r\n\r\n", ctx);
        } else {
            ReplyWithError(NMonitoring::HTTPNOTFOUND, ctx);
        }
    }

private:
    //////////////////////////////////////////////
    // Create task
    //////////////////////////////////////////////
    void ProcessCreateTasksRequest(const NMonitoring::IMonHttpRequest &http, const TActorContext &ctx) {
        NJson::TJsonValue json;

        try {
            ReadJsonTree(http.GetPostContent(), &json, true);
        } catch (yexception e) {
            ReplyWithError(TString("HTTP/1.1 400 Bad Request\r\n\r\nCan't parse provided JSON: ") + e.what(), ctx);
            return;
        }

        TAutoPtr<TEvCms::TEvWalleCreateTaskRequest> request = new TEvCms::TEvWalleCreateTaskRequest;
        auto map = json.GetMap();

        request->Record.SetTaskId(map["id"].GetString());
        request->Record.SetType(map["type"].GetString());
        request->Record.SetIssuer(map["issuer"].GetString());
        request->Record.SetAction(map["action"].GetString());

        auto &hosts = map["hosts"].GetArray();
        for (auto &host : hosts)
            *request->Record.AddHosts() = host.GetString();
        
        auto &devices = map["devices"].GetArray();
        for (auto &device : devices)
            *request->Record.AddDevices() = device.GetString();

        const auto &params = http.GetParams();
        if (params.contains("dry_run"))
            request->Record.SetDryRun(params.find("dry_run")->second == "true");

        SendToCms(request.Release(), ctx);
        Become(&TThis::StateCreateTask, ctx, TDuration::Seconds(60), new TEvents::TEvWakeup());
    }

    STFUNC(StateCreateTask) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCms::TEvWalleCreateTaskResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        default:
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS, "TWalleApiHandler::StateCreateTask ignored event type: %" PRIx32 " event: %s",
                      ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void Reply(const TWalleCreateTaskResponse &resp, const TActorContext &ctx) {
        TString status;
        if (!CheckStatus(resp.GetStatus(), status, false, ctx))
            return;

        NJson::TJsonValue hosts(NJson::JSON_ARRAY);
        for (auto &host : resp.GetHosts())
            hosts.AppendValue(host);

        NJson::TJsonValue devices(NJson::JSON_ARRAY);
        for (auto &device : resp.GetDevices())
            devices.AppendValue(device);

        NJson::TJsonValue map;
        map.InsertValue("id", resp.GetTaskId());
        map.InsertValue("hosts", hosts);
        map.InsertValue("devices", devices);
        map.InsertValue("status", status);
        map.InsertValue("message", resp.GetStatus().GetReason());

        Reply(WriteJson(map), ctx);
    }

    void Handle(TEvCms::TEvWalleCreateTaskResponse::TPtr &ev, const TActorContext &ctx) {
        Reply(ev->Get()->Record, ctx);
    }

    //////////////////////////////////////////////
    // List tasks
    //////////////////////////////////////////////
    void ProcessListTasksRequest(const TActorContext &ctx) {
        TAutoPtr<TEvCms::TEvWalleListTasksRequest> request = new TEvCms::TEvWalleListTasksRequest;

        SendToCms(request.Release(), ctx);
        Become(&TThis::StateListTasks, ctx, TDuration::Seconds(60), new TEvents::TEvWakeup());
    }

    STFUNC(StateListTasks) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCms::TEvWalleListTasksResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        default:
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS, "TWalleApiHandler::StateListTasks ignored event type: %" PRIx32 " event: %s",
                      ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void Reply(const TWalleListTasksResponse &resp, const TActorContext &ctx) {
        NJson::TJsonValue tasks(NJson::JSON_ARRAY);
        for (auto &task : resp.GetTasks()) {

            NJson::TJsonValue hosts(NJson::JSON_ARRAY);
            for (auto &host : task.GetHosts())
                hosts.AppendValue(host);

            NJson::TJsonValue devices(NJson::JSON_ARRAY);
            for (auto &device : task.GetDevices())
                devices.AppendValue(device);

            NJson::TJsonValue map;
            map.InsertValue("id", task.GetTaskId());
            map.InsertValue("hosts", hosts);
            map.InsertValue("devices", devices);
            map.InsertValue("status", task.GetStatus());

            tasks.AppendValue(map);
        }

        NJson::TJsonValue res;
        res.InsertValue("result", tasks);

        Reply(WriteJson(res), ctx);
    }

    void Handle(TEvCms::TEvWalleListTasksResponse::TPtr &ev, const TActorContext &ctx) {
        Reply(ev->Get()->Record, ctx);
    }

    //////////////////////////////////////////////
    // Check task
    //////////////////////////////////////////////
    void ProcessCheckTaskRequest(const TString &id, const TActorContext &ctx) {
        TAutoPtr<TEvCms::TEvWalleCheckTaskRequest> request = new TEvCms::TEvWalleCheckTaskRequest;
        request->Record.SetTaskId(id);

        SendToCms(request.Release(), ctx);
        Become(&TThis::StateCheckTask, ctx, TDuration::Seconds(60), new TEvents::TEvWakeup());
    }

    STFUNC(StateCheckTask) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCms::TEvWalleCheckTaskResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        default:
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS, "TWalleApiHandler::StateCheckTask ignored event type: %" PRIx32 " event: %s",
                      ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void Reply(const TWalleCheckTaskResponse &resp, const TActorContext &ctx) {
        TString status;
        if (!CheckStatus(resp.GetStatus(), status, true, ctx))
            return;

        NJson::TJsonValue hosts(NJson::JSON_ARRAY);
        for (auto &host : resp.GetTask().GetHosts())
            hosts.AppendValue(host);

        NJson::TJsonValue devices(NJson::JSON_ARRAY);
        for (auto &device : resp.GetTask().GetDevices())
            devices.AppendValue(device);

        NJson::TJsonValue map;
        map.InsertValue("id", resp.GetTask().GetTaskId());
        map.InsertValue("hosts", hosts);
        map.InsertValue("devices", devices);
        map.InsertValue("status", status);
        map.InsertValue("message", resp.GetStatus().GetReason());

        Reply(WriteJson(map), ctx);
    }

    void Handle(TEvCms::TEvWalleCheckTaskResponse::TPtr &ev, const TActorContext &ctx) {
        Reply(ev->Get()->Record, ctx);
    }

    //////////////////////////////////////////////
    // Remove task
    //////////////////////////////////////////////
    void ProcessRemoveTaskRequest(const TString &id, const TActorContext &ctx) {
        TAutoPtr<TEvCms::TEvWalleRemoveTaskRequest> request = new TEvCms::TEvWalleRemoveTaskRequest;
        request->Record.SetTaskId(id);

        SendToCms(request.Release(), ctx);
        Become(&TThis::StateRemoveTask, ctx, TDuration::Seconds(60), new TEvents::TEvWakeup());
    }

    STFUNC(StateRemoveTask) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCms::TEvWalleRemoveTaskResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        default:
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS, "TWalleApiHandler::StateRemoveTask ignored event type: %" PRIx32 " event: %s",
                      ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void Reply(const TWalleRemoveTaskResponse &resp, const TActorContext &ctx) {
        TString status;
        if (!CheckStatus(resp.GetStatus(), status, true, ctx))
            return;

        Reply("", ctx);
    }

    void Handle(TEvCms::TEvWalleRemoveTaskResponse::TPtr &ev, const TActorContext &ctx) {
        Reply(ev->Get()->Record, ctx);
    }

    //////////////////////////////////////////////
    // Common section
    //////////////////////////////////////////////
    bool CheckStatus(const TStatus &status, TString &out, bool use404, const TActorContext &ctx) {
        if (status.GetCode() == TStatus::OK
            || status.GetCode() == TStatus::ALLOW) {
            out = "ok";
            return true;
        }

        if (status.GetCode() == TStatus::DISALLOW_TEMP) {
            out = "in-process";
            return true;
        }

        if (status.GetCode() == TStatus::DISALLOW) {
            out = "rejected";
            return true;
        }

        if (status.GetCode() == TStatus::WRONG_REQUEST) {
            TString err;
            if (use404)
                err = NMonitoring::HTTPNOTFOUND;
            else
                err = Sprintf("HTTP/1.1 400 Bad Request\r\n\r\n%s", status.GetReason().data());
            ReplyWithError(err, ctx);
            return false;
        }

        auto err = Sprintf("HTTP/1.1 500 Internal Server Error\r\n\r\n%s", status.GetReason().data());
        ReplyWithError(err, ctx);
        return false;
    }

    void ReplyWithError(const TString &err, const TActorContext &ctx) {
        return ReplyAndDie(err, ctx);
    }

    void Reply(const TString &json, const TActorContext &ctx) {
        const TString header = json ? NMonitoring::HTTPOKJSON : NMonitoring::HTTPNOCONTENT;
        return ReplyAndDie(header + json, ctx);
    }

    void ReplyAndDie(const TString &data, const TActorContext &ctx) {
        AuditLog("Wall-E handler", RequestEvent, data, ctx);
        ctx.Send(RequestEvent->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void Die(const TActorContext &ctx) override {
        NTabletPipe::CloseClient(ctx, CmsPipe);
        TBase::Die(ctx);
    }

    void Timeout(const TActorContext &ctx) {
        ReplyWithError(TString("HTTP/1.1 408 Request Timeout\r\n\r\nCMS request timeout"), ctx);
    }

    void SendToCms(IEventBase *ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(!CmsPipe);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::Seconds(10),
        };
        CmsPipe = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, MakeCmsID(), pipeConfig));
        NTabletPipe::SendData(ctx, CmsPipe, ev);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();

        if (msg->ClientId != CmsPipe || msg->Status == NKikimrProto::OK) {
            return;
        }

        HandlePipeDestroyed(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();

        if (msg->ClientId != CmsPipe) {
            return;
        }

        HandlePipeDestroyed(ctx);
    }

    void HandlePipeDestroyed(const TActorContext &ctx) {
        LOG_WARN(ctx, NKikimrServices::CMS, "TWalleApiHandler::HandlePipeDestroyed");

        NTabletPipe::CloseAndForgetClient(SelfId(), CmsPipe);
        Bootstrap(ctx);
    }

    NMon::TEvHttpInfo::TPtr RequestEvent;
    TActorId CmsPipe;
};

IActor *CreateWalleApiHandler(NMon::TEvHttpInfo::TPtr &event) {
    return new TWalleCrateTaskHandler(event);
}

} // namespace NKikimr::NCms
