#include "health_check.h"

#include "self_check_request.h"

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NHealthCheck {

using namespace NActors;
using namespace Ydb;

template<typename RequestType>
class TNodeCheckRequest : public TActorBootstrapped<TNodeCheckRequest<RequestType>> {
public:
    using TBase = TActorBootstrapped<TNodeCheckRequest<RequestType>>;
    using TThis = TNodeCheckRequest<RequestType>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MONITORING_REQUEST; }

    struct TEvPrivate {
        enum EEv {
            EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvError,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

        struct TEvResult : TEventLocal<TEvResult, EvResult> {
            Ydb::Monitoring::NodeCheckResponse Response;

            TEvResult(Ydb::Monitoring::NodeCheckResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvError : TEventLocal<TEvError, EvError> {
            NYdbGrpc::TGrpcStatus Status;

            TEvError(NYdbGrpc::TGrpcStatus&& status)
                : Status(std::move(status))
            {}
        };
    };

    TDuration Timeout = TDuration::MilliSeconds(10000);
    std::shared_ptr<NYdbGrpc::TGRpcClientLow> GRpcClientLow;
    TActorId Sender;
    THolder<RequestType> Request;
    ui64 Cookie;
    Ydb::Monitoring::SelfCheckResult Result;

    TNodeCheckRequest(std::shared_ptr<NYdbGrpc::TGRpcClientLow> grpcClient, const TActorId& sender, THolder<RequestType> request, ui64 cookie)
        : GRpcClientLow(grpcClient)
        , Sender(sender)
        , Request(std::move(request))
        , Cookie(cookie)
    {
        Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_UNSPECIFIED);
    }

    void Bootstrap();

    void AddIssue(Ydb::Monitoring::StatusFlag::Status status, const TString& message) {
        auto* issue = Result.add_issue_log();
        issue->set_id(std::to_string(Result.issue_log_size()));
        issue->set_status(status);
        issue->set_message(message);
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        NYdbGrpc::TGRpcClientConfig config;
        for (const auto& systemStateInfo : ev->Get()->Record.GetSystemStateInfo()) {
            for (const auto& endpoint : systemStateInfo.GetEndpoints()) {
                if (endpoint.GetName() == "grpc") {
                    config.Locator = "localhost" + endpoint.GetAddress();
                    break;
                } else if (endpoint.GetName() == "grpcs") {
                    config.Locator = "localhost" + endpoint.GetAddress();
                    config.EnableSsl = true;
                    config.SslTargetNameOverride = systemStateInfo.GetHost();
                    break;
                }
            }
            break;
        }
        if (config.Locator.empty()) {
            AddIssue(Ydb::Monitoring::StatusFlag::RED, "Couldn't find local gRPC endpoint");
            ReplyAndPassAway();
        }
        NActors::TActorSystem* actorSystem = TlsActivationContext->ActorSystem();
        NActors::TActorId actorId = TBase::SelfId();
        Ydb::Monitoring::NodeCheckRequest request;
        NYdbGrpc::TResponseCallback<Ydb::Monitoring::NodeCheckResponse> responseCb =
            [actorId, actorSystem, context = GRpcClientLow->CreateContext()](NYdbGrpc::TGrpcStatus&& status, Ydb::Monitoring::NodeCheckResponse&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new typename TEvPrivate::TEvResult(std::move(response)));
            } else {
                actorSystem->Send(actorId, new typename TEvPrivate::TEvError(std::move(status)));
            }
        };
        NYdbGrpc::TCallMeta meta;
        meta.Timeout = Timeout;
        auto service = GRpcClientLow->CreateGRpcServiceConnection<::Ydb::Monitoring::V1::MonitoringService>(config);
        service->DoRequest(request, std::move(responseCb), &Ydb::Monitoring::V1::MonitoringService::Stub::AsyncNodeCheck, meta);
    }

    void Handle(typename TEvPrivate::TEvResult::TPtr& ev) {
        auto& operation(ev->Get()->Response.operation());
        if (operation.ready() && operation.status() == Ydb::StatusIds::SUCCESS) {
            operation.result().UnpackTo(&Result);
        } else {
            Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_MAINTENANCE_REQUIRED);
            AddIssue(Ydb::Monitoring::StatusFlag::RED, "Local gRPC returned error");
        }
        ReplyAndPassAway();
    }

    void Handle(typename TEvPrivate::TEvError::TPtr& ev) {
        Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_MAINTENANCE_REQUIRED);
        AddIssue(Ydb::Monitoring::StatusFlag::RED, "Local gRPC request failed");
        Y_UNUSED(ev);
        ReplyAndPassAway();
    }

    void HandleTimeout() {
        Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_MAINTENANCE_REQUIRED);
        AddIssue(Ydb::Monitoring::StatusFlag::RED, "Timeout");
        ReplyAndPassAway();
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvPrivate::TEvResult, Handle);
            hFunc(TEvPrivate::TEvError, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void FillResult(Ydb::Monitoring::SelfCheckResult& result) {
        result = std::move(Result);
    }

    void ReplyAndPassAway();
};

template<>
void TNodeCheckRequest<TEvNodeCheckRequest>::ReplyAndPassAway() {
    THolder<TEvSelfCheckResult> response = MakeHolder<TEvSelfCheckResult>();
    Ydb::Monitoring::SelfCheckResult& result = response->Result;
    FillResult(result);
    Send(Sender, response.Release(), 0, Cookie);
    PassAway();
}

template<>
void TNodeCheckRequest<NMon::TEvHttpInfo>::ReplyAndPassAway() {
    static const char HTTPJSON_GOOD[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/json\r\n\r\n";
    static const char HTTPJSON_NOT_GOOD[] = "HTTP/1.1 500 Failed\r\nContent-Type: application/json\r\n\r\n";

    Ydb::Monitoring::SelfCheckResult result;
    FillResult(result);
    auto config = NProtobufJson::TProto2JsonConfig()
            .SetFormatOutput(false)
            .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName);
    TStringStream json;
    if (result.self_check_result() == Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_GOOD) {
        json << HTTPJSON_GOOD;
    } else {
        json << HTTPJSON_NOT_GOOD;
    }
    NProtobufJson::Proto2Json(result, json, config);
    Send(Sender, new NMon::TEvHttpInfoRes(json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom), 0, Cookie);
    PassAway();
}

template<>
void TNodeCheckRequest<TEvNodeCheckRequest>::Bootstrap() {
    if (Request->Request.operation_params().has_operation_timeout()) {
        Timeout = GetDuration(Request->Request.operation_params().operation_timeout());
    }
    Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_GOOD);
    ReplyAndPassAway();
}

template<>
void TNodeCheckRequest<NMon::TEvHttpInfo>::Bootstrap() {
    TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(TBase::SelfId().NodeId());
    TBase::Send(whiteboardServiceId, new TEvWhiteboard::TEvSystemStateRequest());
    const auto& params(Request->Request.GetParams());
    Timeout = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("timeout"), Timeout.MilliSeconds()));
    TBase::Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
}

class THealthCheckService : public TActorBootstrapped<THealthCheckService> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MONITORING_SERVICE; }
    NKikimrConfig::THealthCheckConfig HealthCheckConfig;

    THealthCheckService()
    {
    }

    void Bootstrap() {
        HealthCheckConfig.CopyFrom(AppData()->HealthCheckConfig);
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({NKikimrConsole::TConfigItem::HealthCheckConfigItem}));
        TMon* mon = AppData()->Mon;
        if (mon) {
            mon->RegisterActorPage({
                .RelPath = "status",
                .ActorSystem = TActivationContext::ActorSystem(),
                .ActorId = SelfId(),
                .UseAuth = false,
            });
        }
        Become(&THealthCheckService::StateWork);
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetConfig().HasHealthCheckConfig()) {
            HealthCheckConfig.CopyFrom(record.GetConfig().GetHealthCheckConfig());
        }
        Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
    }

    void Handle(TEvSelfCheckRequest::TPtr& ev) {
        Register(new TSelfCheckRequest(ev->Sender, ev.Get()->Release(), ev->Cookie, std::move(ev->TraceId), HealthCheckConfig));
    }

    std::shared_ptr<NYdbGrpc::TGRpcClientLow> GRpcClientLow;

    void Handle(TEvNodeCheckRequest::TPtr& ev) {
        if (!GRpcClientLow) {
            GRpcClientLow = std::make_shared<NYdbGrpc::TGRpcClientLow>();
        }
        Register(new TNodeCheckRequest<TEvNodeCheckRequest>(GRpcClientLow, ev->Sender, ev.Get()->Release(), ev->Cookie));
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        if (ev->Get()->Request.GetPath() == "/status") {
            if (!GRpcClientLow) {
                GRpcClientLow = std::make_shared<NYdbGrpc::TGRpcClientLow>();
            }
            Register(new TNodeCheckRequest<NMon::TEvHttpInfo>(GRpcClientLow, ev->Sender, ev.Get()->Release(), ev->Cookie));
        } else {
            Send(ev->Sender, new NMon::TEvHttpInfoRes(NMonitoring::HTTPNOTFOUND, 0, NMon::IEvHttpInfoRes::EContentType::Custom), 0, ev->Cookie);
        }
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSelfCheckRequest, Handle);
            hFunc(TEvNodeCheckRequest, Handle);
            hFunc(NMon::TEvHttpInfo, Handle);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};

IActor* CreateHealthCheckService() {
    return new THealthCheckService();
}

}
