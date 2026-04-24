#include "actors.h"
#include "common.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/create_topic_operation.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

namespace {

struct TCreateTopicStrategy: public NPQ::NSchema::ICreateTopicStrategy {
    TCreateTopicStrategy(const Ydb::PersQueue::V1::CreateTopicRequest& request, TString&& localDc, TVector<TString>&& clusters)
        : Request(request)
        , LocalDc(std::move(localDc))
        , Clusters(std::move(clusters))
    {
    }

    const TString& GetTopicName() const override {
        return Request.path();
    }

    NPQ::NSchema::TResult ApplyChanges(
        const TString& database,
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig
    ) override {
        auto result = ApplyChangesInt(database, Request, modifyScheme, targetConfig, LocalDc);
        if (!result) {
            return result;
        }

        const auto& pqDescr = modifyScheme.GetCreatePersQueueGroup();
        const auto& config = pqDescr.GetPQTabletConfig();
        if (!LocalDc.empty() && config.GetLocalDC() && config.GetDC() != LocalDc) {
           auto error = TStringBuilder() << "Local cluster is not correct - provided '" << config.GetDC()
                                        << "' instead of " << LocalDc;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
        if (!Clusters.empty() && Count(Clusters, config.GetDC()) == 0)  {
            auto error = TStringBuilder() << "Unknown cluster '" << config.GetDC() << "'";
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
    
        return {};
    }

    const Ydb::PersQueue::V1::CreateTopicRequest Request;
    const TString LocalDc;
    const TVector<TString> Clusters;
};

class TCreateTopicActor: public TGrpcProxyActor<TCreateTopicActor, NGRpcService::TEvPQCreateTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TCreateTopicActor, NGRpcService::TEvPQCreateTopicRequest>;

public:
    TCreateTopicActor(NGRpcService::IRequestOpCtx* request, const TString& localDc, const TVector<TString>& clusters)
        : TGrpcProxyActor<TCreateTopicActor, NGRpcService::TEvPQCreateTopicRequest>(request)
        , LocalDc(localDc)
        , Clusters(clusters)
    {
    }

    void DoAction() {
        Become(&TCreateTopicActor::StateWork);

        auto database = CanonizePath(this->Request_->GetDatabaseName().GetOrElse(""));

        Register(NPQ::NSchema::CreateCreateTopicOperationActor(SelfId(), {
            .Database = database,
            .PeerName = Request_->GetPeerName(),
            .UserToken = GetUserToken(),
            .Strategy = std::make_unique<TCreateTopicStrategy>(*GetProtoRequest(), std::move(LocalDc), std::move(Clusters)),
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvCreateTopicResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyWithError(ev->Get()->Status, ev->Get()->ErrorMessage);
        } else {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::PersQueue::V1::CreateTopicResponse());
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvCreateTopicResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }

private:
    TString LocalDc;
    TVector<TString> Clusters;
};

} // namespace
    
NActors::IActor* CreateCreateTopicActor(NGRpcService::IRequestOpCtx* request, const TString& localDc, const TVector<TString>& clusters) {
    return new TCreateTopicActor(request, localDc, clusters);
}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
