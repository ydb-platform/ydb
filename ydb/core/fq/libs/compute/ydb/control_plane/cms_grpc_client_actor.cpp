#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>

#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/ycloud/api/events.h>
#include <ydb/library/ycloud/impl/grpc_service_client.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [CmsGrpcClient]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [CmsGrpcClient]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [CmsGrpcClient]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [CmsGrpcClient]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [CmsGrpcClient]: " << stream)

namespace NFq {

using namespace NActors;

namespace {

struct TEvPrivate {
    enum EEv {
        // requests
        EvCreateDatabaseRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        // replies
        EvCreateDatabaseResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvCreateDatabaseRequest : NCloud::TEvGrpcProtoRequest<TEvCreateDatabaseRequest, EvCreateDatabaseRequest, Ydb::Cms::CreateDatabaseRequest> {};
    struct TEvCreateDatabaseResponse : NCloud::TEvGrpcProtoResponse<TEvCreateDatabaseResponse, EvCreateDatabaseResponse, Ydb::Cms::CreateDatabaseResponse> {};
};

}

class TCmsGrpcServiceActor : public NActors::TActor<TCmsGrpcServiceActor>, TGrpcServiceClient<Ydb::Cms::V1::CmsService> {
public:
    using TBase = NActors::TActor<TCmsGrpcServiceActor>;
    struct TCreateDatabaseGrpcRequest : TGrpcRequest {
        static constexpr auto Request = &Ydb::Cms::V1::CmsService::Stub::AsyncCreateDatabase;
        using TRequestEventType = TEvPrivate::TEvCreateDatabaseRequest;
        using TResponseEventType = TEvPrivate::TEvCreateDatabaseResponse;
    };

    TCmsGrpcServiceActor(const NCloud::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider)
        : TBase(&TCmsGrpcServiceActor::StateFunc)
        , TGrpcServiceClient(settings)
        , Settings(settings)
        , CredentialsProvider(credentialsProvider)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCreateDatabaseRequest, Handle);
        hFunc(TEvPrivate::TEvCreateDatabaseResponse, Handle);
    )

    void Handle(TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& ev) {
        const auto& request = *ev.Get()->Get();
        auto forwardRequest = std::make_unique<TEvPrivate::TEvCreateDatabaseRequest>();
        forwardRequest->Request.mutable_serverless_resources()->set_shared_database_path(request.BasePath);
        forwardRequest->Request.set_path(request.Path);
        forwardRequest->Token = CredentialsProvider->GetAuthInfo();
        TEvPrivate::TEvCreateDatabaseRequest::TPtr forwardEvent = (NActors::TEventHandle<TEvPrivate::TEvCreateDatabaseRequest>*)new IEventHandle(SelfId(), SelfId(), forwardRequest.release(), 0, Cookie);
        MakeCall<TCreateDatabaseGrpcRequest>(std::move(forwardEvent));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvPrivate::TEvCreateDatabaseResponse::TPtr& ev) {
        const auto& status = ev->Get()->Status;
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist. Need to fix this bug urgently");
            return;
        }
        auto request = it->second;
        Requests.erase(it);

        auto forwardResponse = std::make_unique<TEvYdbCompute::TEvCreateDatabaseResponse>();
        if (!status.Ok() && status.GRpcStatusCode != grpc::StatusCode::ALREADY_EXISTS) {
            forwardResponse->Issues.AddIssue("GrpcCode: " + ToString(status.GRpcStatusCode));
            forwardResponse->Issues.AddIssue("Message: " + status.Msg);
            forwardResponse->Issues.AddIssue("Details: " + status.Details);
            Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
            return;
        }

        forwardResponse->Result.set_id(request.Get()->Get()->Path);
        forwardResponse->Result.mutable_connection()->set_endpoint(Settings.Endpoint);
        forwardResponse->Result.mutable_connection()->set_database(request.Get()->Get()->Path);
        forwardResponse->Result.mutable_connection()->set_usessl(Settings.EnableSsl);

        Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
    }

private:
    NCloud::TGrpcClientSettings Settings;
    TMap<uint64_t, TEvYdbCompute::TEvCreateDatabaseRequest::TPtr> Requests;
    NYdb::TCredentialsProviderPtr CredentialsProvider;
    int64_t Cookie = 0;
};

std::unique_ptr<NActors::IActor> CreateCmsGrpcClientActor(const NCloud::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider) {
    return std::make_unique<TCmsGrpcServiceActor>(settings, credentialsProvider);
}

}
