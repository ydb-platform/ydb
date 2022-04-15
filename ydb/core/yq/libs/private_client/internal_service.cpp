#include "internal_service.h"
#include "private_client.h"

#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>

#include <ydb/core/protos/services.pb.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)
#define LOG_W(stream) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)
#define LOG_I(stream) \
    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_INTERNAL_SERVICE, stream)

namespace NYq {

NActors::TActorId MakeInternalServiceActorId() {
    constexpr TStringBuf name = "FQINTSRV";
    return NActors::TActorId(0, name);
}

class TInternalService : public NActors::TActorBootstrapped<TInternalService> {
public:
    TInternalService(
        const NYq::TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NYq::NConfig::TPrivateApiConfig& privateApiConfig,
        const NMonitoring::TDynamicCounterPtr& counters)
        : ServiceCounters(counters->GetSubgroup("subsystem", "InternalService"))
        , EventLatency(ServiceCounters->GetSubgroup("subcomponent", "Latency")->GetHistogram("Latency", NMonitoring::ExponentialHistogram(10, 2, 50)))
        , PrivateClient(
            yqSharedResources->CoreYdbDriver,
            NYdb::TCommonClientSettings()
                .DiscoveryEndpoint(privateApiConfig.GetTaskServiceEndpoint())
                .CredentialsProviderFactory(credentialsProviderFactory({.SaKeyFile = privateApiConfig.GetSaKeyFile(), .IamEndpoint = privateApiConfig.GetIamEndpoint()}))
                .EnableSsl(privateApiConfig.GetSecureTaskService())
                .Database(privateApiConfig.GetTaskServiceDatabase() ? privateApiConfig.GetTaskServiceDatabase() : TMaybe<TString>()),
            counters)
    {
    }

    static constexpr char ActorName[] = "FQ_INTERNAL_SERVICE";

    void Bootstrap() {
        Become(&TInternalService::StateFunc);
        LOG_I("STARTED");
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvInternalService::TEvHealthCheckRequest, Handle)
        hFunc(TEvInternalService::TEvGetTaskRequest, Handle)
        hFunc(TEvInternalService::TEvPingTaskRequest, Handle)
        hFunc(TEvInternalService::TEvWriteResultRequest, Handle)
    );

    void Handle(TEvInternalService::TEvHealthCheckRequest::TPtr& ev) {
        EventLatency->Collect((TInstant::Now() - ev->Get()->SentAt).MilliSeconds());
        const auto actorSystem = NActors::TActivationContext::ActorSystem();
        const auto senderId = ev->Sender;
        PrivateClient
            .NodesHealthCheck(std::move(ev->Get()->Request))
            .Subscribe([actorSystem, senderId](const NThreading::TFuture<TNodesHealthCheckResult>& future) {
                try {
                    const auto& wrappedResult = future.GetValue();
                    if (wrappedResult.IsResultSet()) {
                        actorSystem->Send(senderId, new TEvInternalService::TEvHealthCheckResponse(
                            wrappedResult.IsSuccess(), wrappedResult.GetStatus(), wrappedResult.GetIssues(), wrappedResult.GetResult())
                        );
                    } else {
                        actorSystem->Send(senderId, new TEvInternalService::TEvHealthCheckResponse(
                            false, wrappedResult.GetStatus(), NYql::TIssues{{NYql::TIssue{"grpc private api result is not set for health check call"}}}, Yq::Private::NodesHealthCheckResult{})
                        );
                    }
                } catch (...) {
                    actorSystem->Send(senderId, new TEvInternalService::TEvHealthCheckResponse(
                        false, NYdb::EStatus::STATUS_UNDEFINED, NYql::TIssues{{NYql::TIssue{CurrentExceptionMessage()}}}, Yq::Private::NodesHealthCheckResult{})
                    );
                }
            });
    }

    void Handle(TEvInternalService::TEvGetTaskRequest::TPtr& ev) {
        EventLatency->Collect((TInstant::Now() - ev->Get()->SentAt).MilliSeconds());
        const auto actorSystem = NActors::TActivationContext::ActorSystem();
        const auto senderId = ev->Sender;
        PrivateClient
            .GetTask(std::move(ev->Get()->Request))
            .Subscribe([actorSystem, senderId](const NThreading::TFuture<TGetTaskResult>& future) {
                try {
                    const auto& wrappedResult = future.GetValue();
                    if (wrappedResult.IsResultSet()) {
                        actorSystem->Send(senderId, new TEvInternalService::TEvGetTaskResponse(
                            wrappedResult.IsSuccess(), wrappedResult.GetStatus(), wrappedResult.GetIssues(), wrappedResult.GetResult())
                        );
                    } else {
                        actorSystem->Send(senderId, new TEvInternalService::TEvGetTaskResponse(
                            false, wrappedResult.GetStatus(), NYql::TIssues{{NYql::TIssue{"grpc private api result is not set for get task call"}}}, Yq::Private::GetTaskResult{})
                        );
                    }
                } catch (...) {
                    actorSystem->Send(senderId, new TEvInternalService::TEvGetTaskResponse(
                        false, NYdb::EStatus::STATUS_UNDEFINED, NYql::TIssues{{NYql::TIssue{CurrentExceptionMessage()}}}, Yq::Private::GetTaskResult{})
                    );
                }
            });
    }

    void Handle(TEvInternalService::TEvPingTaskRequest::TPtr& ev) {
        EventLatency->Collect((TInstant::Now() - ev->Get()->SentAt).MilliSeconds());
        const auto actorSystem = NActors::TActivationContext::ActorSystem();
        const auto senderId = ev->Sender;
        const auto selfId = SelfId();
        PrivateClient
            .PingTask(std::move(ev->Get()->Request))
            .Subscribe([actorSystem, senderId, selfId, cookie=ev->Cookie](const NThreading::TFuture<TPingTaskResult>& future) {
                try {
                    const auto& wrappedResult = future.GetValue();
                    if (wrappedResult.IsResultSet()) {
                        actorSystem->Send(new NActors::IEventHandle(senderId, selfId, new TEvInternalService::TEvPingTaskResponse(
                            wrappedResult.IsSuccess(), wrappedResult.GetStatus(), wrappedResult.GetIssues(), wrappedResult.GetResult(), wrappedResult.IsTransportError())
                        , 0, cookie));
                    } else {
                        actorSystem->Send(new NActors::IEventHandle(senderId, selfId, new TEvInternalService::TEvPingTaskResponse(
                            false, wrappedResult.GetStatus(), NYql::TIssues{{NYql::TIssue{"grpc private api result is not set for ping task call"}}}, Yq::Private::PingTaskResult{})
                        , 0, cookie));
                    }
                } catch (...) {
                    actorSystem->Send(new NActors::IEventHandle(senderId, selfId, new TEvInternalService::TEvPingTaskResponse(
                        false, NYdb::EStatus::STATUS_UNDEFINED, NYql::TIssues{{NYql::TIssue{CurrentExceptionMessage()}}}, Yq::Private::PingTaskResult{})
                    , 0, cookie));
                }
            });
    }

    void Handle(TEvInternalService::TEvWriteResultRequest::TPtr& ev) {
        EventLatency->Collect((TInstant::Now() - ev->Get()->SentAt).MilliSeconds());
        const auto actorSystem = NActors::TActivationContext::ActorSystem();
        const auto senderId = ev->Sender;
        PrivateClient
            .WriteTaskResult(std::move(ev->Get()->Request))
            .Subscribe([actorSystem, senderId](const auto& future) {
                try {
                    const auto& wrappedResult = future.GetValue();
                    if (wrappedResult.IsResultSet()) {
                        actorSystem->Send(senderId, new TEvInternalService::TEvWriteResultResponse(
                            wrappedResult.IsSuccess(), wrappedResult.GetStatus(), wrappedResult.GetIssues(), wrappedResult.GetResult())
                        );
                    } else {
                        actorSystem->Send(senderId, new TEvInternalService::TEvWriteResultResponse(
                            false, wrappedResult.GetStatus(), NYql::TIssues{{NYql::TIssue{"grpc private api result is not set for write result task call"}}}, Yq::Private::WriteTaskResultResult{})
                        );
                    }
                } catch (...) {
                    actorSystem->Send(senderId, new TEvInternalService::TEvWriteResultResponse(
                        false, NYdb::EStatus::STATUS_UNDEFINED, NYql::TIssues{{NYql::TIssue{CurrentExceptionMessage()}}}, Yq::Private::WriteTaskResultResult{})
                    );
                }
            });
    }

    const NMonitoring::TDynamicCounterPtr ServiceCounters;
    const NMonitoring::THistogramPtr EventLatency;
    TPrivateClient PrivateClient;
};

NActors::IActor* CreateInternalServiceActor(
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const NYq::NConfig::TPrivateApiConfig& privateApiConfig,
    const NMonitoring::TDynamicCounterPtr& counters) {
        return new TInternalService(yqSharedResources, credentialsProviderFactory, privateApiConfig, counters);
}

} /* NYq */
