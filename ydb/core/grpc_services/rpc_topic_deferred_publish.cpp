#include "service_topic_deferred_publish.h"
#include "rpc_calls_topic_deferred_publish.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/persqueue/deferred_publish/events.h>
#include <ydb/core/persqueue/deferred_publish/registry_actor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NGRpcService {

namespace {

constexpr TStringBuf NotImplementedMessage = "Topic deferred publish is not implemented yet";
constexpr TStringBuf DisabledMessage = "Topic deferred publish is not enabled";

TString GetUserSID(const IRequestOpCtx* request) {
    if (request == nullptr) {
        return BUILTIN_ACL_NO_USER_SID;
    }
    return (request->GetInternalToken() != nullptr)
        ? request->GetInternalToken()->GetUserSID()
        : TString(BUILTIN_ACL_NO_USER_SID);
}

template <typename TEvRequest, typename TResult>
class TTopicDeferredPublishNotImplementedRPC
    : public NActors::TActorBootstrapped<TTopicDeferredPublishNotImplementedRPC<TEvRequest, TResult>> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TTopicDeferredPublishNotImplementedRPC(IRequestOpCtx* request)
        : Request(request)
    {}

    void Bootstrap() {
        Request->RaiseIssue(NYql::TIssue(TString(NotImplementedMessage)));
        TResult result;
        Request->SendResult(result, Ydb::StatusIds::UNSUPPORTED);
        this->PassAway();
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
};

template <typename TEvRequest, typename TResult>
void RegisterNotImplementedRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TTopicDeferredPublishNotImplementedRPC<TEvRequest, TResult>(p.release()));
}

class TBeginPublicationRequestActor
    : public NActors::TActorBootstrapped<TBeginPublicationRequestActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TBeginPublicationRequestActor(IRequestOpCtx* request)
        : Request(request)
    {}

    void Bootstrap() {
        if (!AppData()->FeatureFlags.GetEnableTopicDeferredPublish()) {
            Request->RaiseIssue(NYql::TIssue(TString(DisabledMessage)));
            Ydb::Topic::DeferredPublish::BeginPublicationResult result;
            Request->SendResult(result, Ydb::StatusIds::UNSUPPORTED);
            PassAway();
            return;
        }

        const auto database = Request->GetDatabaseName();
        if (!database || database->empty()) {
            Request->RaiseIssue(NYql::TIssue("Database name is not set"));
            Ydb::Topic::DeferredPublish::BeginPublicationResult result;
            Request->SendResult(result, Ydb::StatusIds::BAD_REQUEST);
            PassAway();
            return;
        }

        const auto* protoRequest = TEvBeginPublicationRequest::GetProtoRequest(Request);
        if (protoRequest->ext_publication_id().empty()) {
            Request->RaiseIssue(NYql::TIssue("ext_publication_id must not be empty"));
            Ydb::Topic::DeferredPublish::BeginPublicationResult result;
            Request->SendResult(result, Ydb::StatusIds::BAD_REQUEST);
            PassAway();
            return;
        }

        TMaybe<TString> writerIdentity;
        if (protoRequest->has_writer_identity()) {
            writerIdentity = protoRequest->writer_identity();
        }

        const auto registryId = NPQ::NDeferredPublish::GetOrCreateDeferredPublishRegistryActorId(
            TlsActivationContext->ActorSystem());
        Send(registryId,
            [&] {
                auto* event = new NPQ::NDeferredPublish::TEvBeginPublicationRequest;
                event->Database = *database;
                event->ExtPublicationId = protoRequest->ext_publication_id();
                event->WriterIdentity = writerIdentity;
                event->CreatedBy = GetUserSID(Request.get());
                return event;
            }());
        Become(&TBeginPublicationRequestActor::StateFunc);
    }

    void Handle(NPQ::NDeferredPublish::TEvBeginPublicationResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            for (const auto& issue : ev->Get()->Issues) {
                Request->RaiseIssue(issue);
            }
            if (ev->Get()->Issues.Empty()) {
                Request->RaiseIssue(NYql::TIssue("BeginPublication failed"));
            }
        }

        Ydb::Topic::DeferredPublish::BeginPublicationResult result;
        result.set_int_publication_id(ev->Get()->IntPublicationId);
        Request->SendResult(result, ev->Get()->Status);
        PassAway();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NDeferredPublish::TEvBeginPublicationResponse, Handle);
            default:
                break;
        }
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
};

} // namespace

void DoBeginPublicationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TBeginPublicationRequestActor(p.release()));
}

void DoPublishRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    RegisterNotImplementedRequest<TEvPublishRequest, Ydb::Topic::DeferredPublish::PublishResult>(std::move(p), f);
}

void DoCancelPublicationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    RegisterNotImplementedRequest<TEvCancelPublicationRequest, Ydb::Topic::DeferredPublish::CancelPublicationResult>(std::move(p), f);
}

void DoListPublicationsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    RegisterNotImplementedRequest<TEvListPublicationsRequest, Ydb::Topic::DeferredPublish::ListPublicationsResult>(std::move(p), f);
}

void DoDescribePublicationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    RegisterNotImplementedRequest<TEvDescribePublicationRequest, Ydb::Topic::DeferredPublish::DescribePublicationResult>(std::move(p), f);
}

}
