#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>

#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/ycloud/api/events.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringGrpcClient]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringGrpcClient]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringGrpcClient]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringGrpcClient]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringGrpcClient]: " << stream)

namespace NFq {

using namespace NActors;

namespace {

struct TEvPrivate {
    enum EEv {
        EvSelfCheckRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvSelfCheckResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvSelfCheckRequest : NCloud::TEvGrpcProtoRequest<TEvSelfCheckRequest, EvSelfCheckRequest, Ydb::Monitoring::SelfCheckRequest> {};
    struct TEvSelfCheckResponse : NCloud::TEvGrpcProtoResponse<TEvSelfCheckResponse, EvSelfCheckResponse, Ydb::Monitoring::SelfCheckResponse> {};
};

}

class TMonitoringGrpcServiceActor : public NActors::TActor<TMonitoringGrpcServiceActor>, NGrpcActorClient::TGrpcServiceClient<Ydb::Monitoring::V1::MonitoringService> {
public:
    using TBase = NActors::TActor<TMonitoringGrpcServiceActor>;
    struct TSelfCheckGrpcRequest : TGrpcRequest {
        static constexpr auto Request = &Ydb::Monitoring::V1::MonitoringService::Stub::AsyncSelfCheck;
        using TRequestEventType = TEvPrivate::TEvSelfCheckRequest;
        using TResponseEventType = TEvPrivate::TEvSelfCheckResponse;
    };

    TMonitoringGrpcServiceActor(const NGrpcActorClient::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider)
        : TBase(&TMonitoringGrpcServiceActor::StateFunc)
        , TGrpcServiceClient(settings)
        , Settings(settings)
        , CredentialsProvider(credentialsProvider)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCpuLoadRequest, Handle);
        hFunc(TEvPrivate::TEvSelfCheckResponse, Handle);
    )

    void Handle(TEvYdbCompute::TEvCpuLoadRequest::TPtr& ev) {
        auto forwardRequest = std::make_unique<TEvPrivate::TEvSelfCheckRequest>();
        forwardRequest->Request.set_return_verbose_status(true);
        forwardRequest->Token = CredentialsProvider->GetAuthInfo();
        TEvPrivate::TEvSelfCheckRequest::TPtr forwardEvent = (NActors::TEventHandle<TEvPrivate::TEvSelfCheckRequest>*)new IEventHandle(SelfId(), SelfId(), forwardRequest.release(), 0, Cookie);
        MakeCall<TSelfCheckGrpcRequest>(std::move(forwardEvent));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvPrivate::TEvSelfCheckResponse::TPtr& ev) {
        const auto& status = ev->Get()->Status;

        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (SelfCheckResponse). Need to fix this bug urgently");
            return;
        }
        auto request = it->second;
        Requests.erase(it);

        auto forwardResponse = std::make_unique<TEvYdbCompute::TEvCpuLoadResponse>();
        if (!status.Ok()) {
            forwardResponse->Issues.AddIssue("GrpcCode: " + ToString(status.GRpcStatusCode));
            forwardResponse->Issues.AddIssue("Message: " + status.Msg);
            forwardResponse->Issues.AddIssue("Details: " + status.Details);
            Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
            return;
        }

        Ydb::Monitoring::SelfCheckResult response;
        ev->Get()->Response.operation().result().UnpackTo(&response);

        double totalLoad = 0.0;
        double nodeCount = 0;

        for (auto& databaseStatus : response.database_status()) {
            for (auto& nodeStatus : databaseStatus.compute().nodes()) {
                for (auto& poolStatus : nodeStatus.pools()) {
                    if (poolStatus.name() == "User") {
                        totalLoad += poolStatus.usage();
                        nodeCount++;
                        break;
                    }
                }
            }
        }

        if (nodeCount) {
            forwardResponse->InstantLoad = totalLoad / nodeCount;
        } else {
            forwardResponse->Issues.AddIssue("User pool node load missed");
        }

        Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
    }

private:
    NGrpcActorClient::TGrpcClientSettings Settings;
    TMap<uint64_t, TEvYdbCompute::TEvCpuLoadRequest::TPtr> Requests;
    NYdb::TCredentialsProviderPtr CredentialsProvider;
    int64_t Cookie = 0;
};

std::unique_ptr<NActors::IActor> CreateMonitoringGrpcClientActor(const NGrpcActorClient::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider) {
    return std::make_unique<TMonitoringGrpcServiceActor>(settings, credentialsProvider);
}

}
