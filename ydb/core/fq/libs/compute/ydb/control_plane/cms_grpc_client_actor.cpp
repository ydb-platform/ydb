#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>

#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/ycloud/api/events.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

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
        EvCreateDatabaseRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateDatabaseResponse,
        EvListDatabasesRequest,
        EvListDatabasesResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvCreateDatabaseRequest : NCloud::TEvGrpcProtoRequest<TEvCreateDatabaseRequest, EvCreateDatabaseRequest, Ydb::Cms::CreateDatabaseRequest> {};
    struct TEvCreateDatabaseResponse : NCloud::TEvGrpcProtoResponse<TEvCreateDatabaseResponse, EvCreateDatabaseResponse, Ydb::Cms::CreateDatabaseResponse> {};
    struct TEvListDatabasesRequest : NCloud::TEvGrpcProtoRequest<TEvListDatabasesRequest, EvListDatabasesRequest, Ydb::Cms::ListDatabasesRequest> {};
    struct TEvListDatabasesResponse : NCloud::TEvGrpcProtoResponse<TEvListDatabasesResponse, EvListDatabasesResponse, Ydb::Cms::ListDatabasesResponse> {};
};

}

class TCmsGrpcServiceActor : public NActors::TActor<TCmsGrpcServiceActor>, NGrpcActorClient::TGrpcServiceClient<Ydb::Cms::V1::CmsService> {
public:
    using TBase = NActors::TActor<TCmsGrpcServiceActor>;
    struct TCreateDatabaseGrpcRequest : TGrpcRequest {
        static constexpr auto Request = &Ydb::Cms::V1::CmsService::Stub::AsyncCreateDatabase;
        using TRequestEventType = TEvPrivate::TEvCreateDatabaseRequest;
        using TResponseEventType = TEvPrivate::TEvCreateDatabaseResponse;
    };

    struct TListDatabasesGrpcRequest : TGrpcRequest {
        static constexpr auto Request = &Ydb::Cms::V1::CmsService::Stub::AsyncListDatabases;
        using TRequestEventType = TEvPrivate::TEvListDatabasesRequest;
        using TResponseEventType = TEvPrivate::TEvListDatabasesResponse;
    };

    TCmsGrpcServiceActor(const NGrpcActorClient::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider)
        : TBase(&TCmsGrpcServiceActor::StateFunc)
        , TGrpcServiceClient(settings)
        , Settings(settings)
        , CredentialsProvider(credentialsProvider)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCreateDatabaseRequest, Handle);
        hFunc(TEvPrivate::TEvCreateDatabaseResponse, Handle);
        hFunc(TEvYdbCompute::TEvListDatabasesRequest, Handle);
        hFunc(TEvPrivate::TEvListDatabasesResponse, Handle);
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
            LOG_E("Request doesn't exist (CreateDatabaseResponse). Need to fix this bug urgently");
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvYdbCompute::TEvCreateDatabaseRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            LOG_E("Request differs from the CreateDatabaseRequest type. Need to fix this bug urgently");
            return;
        }
        auto request = *requestPtr;

        auto forwardResponse = std::make_unique<TEvYdbCompute::TEvCreateDatabaseResponse>();
        if (!status.Ok() && status.GRpcStatusCode != grpc::StatusCode::ALREADY_EXISTS) {
            forwardResponse->Issues.AddIssue("GrpcCode: " + ToString(status.GRpcStatusCode));
            forwardResponse->Issues.AddIssue("Message: " + status.Msg);
            forwardResponse->Issues.AddIssue("Details: " + status.Details);
            Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
            return;
        }

        forwardResponse->Result.set_id(request.Get()->Get()->Path);
        forwardResponse->Result.mutable_connection()->set_endpoint(request->Get()->ExecutionConnection.GetEndpoint());
        forwardResponse->Result.mutable_connection()->set_database(request.Get()->Get()->Path);
        forwardResponse->Result.mutable_connection()->set_usessl(request->Get()->ExecutionConnection.GetUseSsl());

        Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
    }

    void Handle(TEvYdbCompute::TEvListDatabasesRequest::TPtr& ev) {
        auto forwardRequest = std::make_unique<TEvPrivate::TEvListDatabasesRequest>();
        forwardRequest->Token = CredentialsProvider->GetAuthInfo();
        TEvPrivate::TEvListDatabasesRequest::TPtr forwardEvent = (NActors::TEventHandle<TEvPrivate::TEvListDatabasesRequest>*)new IEventHandle(SelfId(), SelfId(), forwardRequest.release(), 0, Cookie);
        MakeCall<TListDatabasesGrpcRequest>(std::move(forwardEvent));
        Requests[Cookie++] = ev;
    }

    void Handle(TEvPrivate::TEvListDatabasesResponse::TPtr& ev) {
        const auto& status = ev->Get()->Status;
        Ydb::Cms::ListDatabasesResult response;
        ev->Get()->Response.operation().result().UnpackTo(&response);

        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (ListDatabasesResponse). Need to fix this bug urgently");
            return;
        }
        auto requestVariant = it->second;
        Requests.erase(it);
        const auto* requestPtr = std::get_if<TEvYdbCompute::TEvListDatabasesRequest::TPtr>(&requestVariant);
        if (!requestPtr) {
            LOG_E("Request differs from the ListDatabasesRequest type. Need to fix this bug urgently");
            return;
        }
        auto request = *requestPtr;

        auto forwardResponse = std::make_unique<TEvYdbCompute::TEvListDatabasesResponse>();
        if (!status.Ok()) {
            forwardResponse->Issues.AddIssue("GrpcCode: " + ToString(status.GRpcStatusCode));
            forwardResponse->Issues.AddIssue("Message: " + status.Msg);
            forwardResponse->Issues.AddIssue("Details: " + status.Details);
            Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
            return;
        }

        forwardResponse->Paths.insert(response.paths().begin(), response.paths().end());

        Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
    }

private:
    NGrpcActorClient::TGrpcClientSettings Settings;
    TMap<uint64_t, std::variant<TEvYdbCompute::TEvCreateDatabaseRequest::TPtr, TEvYdbCompute::TEvListDatabasesRequest::TPtr>> Requests;
    NYdb::TCredentialsProviderPtr CredentialsProvider;
    int64_t Cookie = 0;
};

std::unique_ptr<NActors::IActor> CreateCmsGrpcClientActor(const NGrpcActorClient::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider) {
    return std::make_unique<TCmsGrpcServiceActor>(settings, credentialsProvider);
}

}
