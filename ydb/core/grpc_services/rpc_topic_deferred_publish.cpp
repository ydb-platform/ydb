#include "service_topic_deferred_publish.h"
#include "rpc_calls_topic_deferred_publish.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/persqueue/deferred_publish/describe_publication_query.h>
#include <ydb/core/persqueue/deferred_publish/events.h>
#include <ydb/core/persqueue/deferred_publish/list_publications_query.h>
#include <ydb/core/persqueue/deferred_publish/registry_actor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <google/protobuf/timestamp.pb.h>

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

void SetTimestamp(google::protobuf::Timestamp* timestamp, TInstant value) {
    timestamp->set_seconds(value.Seconds());
    timestamp->set_nanos(value.NanoSecondsOfSecond());
}

void RaiseIssues(IRequestOpCtx* request, const NYql::TIssues& issues, const TString& fallbackMessage) {
    for (const auto& issue : issues) {
        request->RaiseIssue(issue);
    }
    if (issues.Empty()) {
        request->RaiseIssue(NYql::TIssue(fallbackMessage));
    }
}

template <typename TEvRequest, typename TResult>
class TTopicDeferredPublishDisabledRPC
    : public NActors::TActorBootstrapped<TTopicDeferredPublishDisabledRPC<TEvRequest, TResult>> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TTopicDeferredPublishDisabledRPC(IRequestOpCtx* request)
        : Request(request)
    {}

    void Bootstrap() {
        Request->RaiseIssue(NYql::TIssue(TString(DisabledMessage)));
        TResult result;
        Request->SendResult(result, Ydb::StatusIds::UNSUPPORTED);
        this->PassAway();
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
};

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
void RegisterDeferredPublishNotImplementedRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    if (!AppData()->FeatureFlags.GetEnableTopicDeferredPublish()) {
        f.RegisterActor(new TTopicDeferredPublishDisabledRPC<TEvRequest, TResult>(p.release()));
        return;
    }
    f.RegisterActor(new TTopicDeferredPublishNotImplementedRPC<TEvRequest, TResult>(p.release()));
}

bool ValidateDeferredPublishDatabase(
    IRequestOpCtx* request,
    const TMaybe<TString>& database)
{
    if (!database || database->empty()) {
        request->RaiseIssue(NYql::TIssue("Database name is not set"));
        return false;
    }
    return true;
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
        if (!ValidateDeferredPublishDatabase(Request.get(), database)) {
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

        Send(NPQ::NDeferredPublish::MakeDeferredPublishRegistryActorId(),
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
            RaiseIssues(Request.get(), ev->Get()->Issues, "BeginPublication failed");
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

class TListPublicationsRequestActor
    : public NActors::TActorBootstrapped<TListPublicationsRequestActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TListPublicationsRequestActor(IRequestOpCtx* request)
        : Request(request)
    {}

    void Bootstrap() {
        if (!AppData()->FeatureFlags.GetEnableTopicDeferredPublish()) {
            Request->RaiseIssue(NYql::TIssue(TString(DisabledMessage)));
            Ydb::Topic::DeferredPublish::ListPublicationsResult result;
            Request->SendResult(result, Ydb::StatusIds::UNSUPPORTED);
            PassAway();
            return;
        }

        const auto database = Request->GetDatabaseName();
        if (!ValidateDeferredPublishDatabase(Request.get(), database)) {
            Ydb::Topic::DeferredPublish::ListPublicationsResult result;
            Request->SendResult(result, Ydb::StatusIds::BAD_REQUEST);
            PassAway();
            return;
        }

        const auto* protoRequest = TEvListPublicationsRequest::GetProtoRequest(Request);
        TMaybe<TString> writerIdentityFilter;
        if (protoRequest->has_writer_identity()) {
            writerIdentityFilter = protoRequest->writer_identity();
        }

        Register(NPQ::NDeferredPublish::CreateListPublicationsQueryActor(
            SelfId(), *database, writerIdentityFilter));
        Become(&TListPublicationsRequestActor::StateFunc);
    }

    void Handle(NPQ::NDeferredPublish::TEvListPublicationsResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            RaiseIssues(Request.get(), ev->Get()->Issues, "ListPublications failed");
        }

        Ydb::Topic::DeferredPublish::ListPublicationsResult result;
        for (const auto& publication : ev->Get()->Publications) {
            auto* summary = result.add_publications();
            summary->set_int_publication_id(publication.IntPublicationId);
            summary->set_ext_publication_id(publication.ExtPublicationId);
            if (publication.WriterIdentity) {
                summary->set_writer_identity(*publication.WriterIdentity);
            }
        }

        Request->SendResult(result, ev->Get()->Status);
        PassAway();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NDeferredPublish::TEvListPublicationsResponse, Handle);
            default:
                break;
        }
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
};

class TDescribePublicationRequestActor
    : public NActors::TActorBootstrapped<TDescribePublicationRequestActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TDescribePublicationRequestActor(IRequestOpCtx* request)
        : Request(request)
    {}

    void Bootstrap() {
        if (!AppData()->FeatureFlags.GetEnableTopicDeferredPublish()) {
            Request->RaiseIssue(NYql::TIssue(TString(DisabledMessage)));
            Ydb::Topic::DeferredPublish::DescribePublicationResult result;
            Request->SendResult(result, Ydb::StatusIds::UNSUPPORTED);
            PassAway();
            return;
        }

        const auto database = Request->GetDatabaseName();
        if (!ValidateDeferredPublishDatabase(Request.get(), database)) {
            Ydb::Topic::DeferredPublish::DescribePublicationResult result;
            Request->SendResult(result, Ydb::StatusIds::BAD_REQUEST);
            PassAway();
            return;
        }

        const auto* protoRequest = TEvDescribePublicationRequest::GetProtoRequest(Request);
        Register(NPQ::NDeferredPublish::CreateDescribePublicationQueryActor(
            SelfId(), *database, protoRequest->int_publication_id()));
        Become(&TDescribePublicationRequestActor::StateFunc);
    }

    void Handle(NPQ::NDeferredPublish::TEvDescribePublicationResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            RaiseIssues(Request.get(), ev->Get()->Issues, "DescribePublication failed");
        }

        Ydb::Topic::DeferredPublish::DescribePublicationResult result;
        if (ev->Get()->Publication) {
            const auto& publication = *ev->Get()->Publication;
            result.set_ext_publication_id(publication.ExtPublicationId);
            if (publication.WriterIdentity) {
                result.set_writer_identity(*publication.WriterIdentity);
            }
            SetTimestamp(result.mutable_created_at(), publication.CreatedAt);
            if (publication.CreatedBy) {
                result.set_created_by(*publication.CreatedBy);
            }
            for (const auto& destination : publication.Destinations) {
                auto* protoDestination = result.add_destinations();
                protoDestination->set_topic_path(destination.TopicPath);
                for (const i64 partitionId : destination.PartitionIds) {
                    protoDestination->add_partition_ids(partitionId);
                }
            }
        }

        Request->SendResult(result, ev->Get()->Status);
        PassAway();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NDeferredPublish::TEvDescribePublicationResponse, Handle);
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
    RegisterDeferredPublishNotImplementedRequest<TEvPublishRequest, Ydb::Topic::DeferredPublish::PublishResult>(std::move(p), f);
}

void DoCancelPublicationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    RegisterDeferredPublishNotImplementedRequest<TEvCancelPublicationRequest, Ydb::Topic::DeferredPublish::CancelPublicationResult>(std::move(p), f);
}

void DoListPublicationsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TListPublicationsRequestActor(p.release()));
}

void DoDescribePublicationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribePublicationRequestActor(p.release()));
}

}
