#include "actors.h"
#include "common.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/schema/alter_topic_operation.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

namespace {

using namespace NPQ::NSchema;

struct TAlterTopicStrategy: public IAlterTopicStrategy {
    TAlterTopicStrategy(
        const TString& database,
        TString&& localCluster,
        const Ydb::PersQueue::V1::AlterTopicRequest& request
    )
        : Database(database)
        , LocalCluster(localCluster)
        , Request(request)
    {
    }

    const TString& GetTopicName() const override {
        return Request.path();
    }

    TResult ApplyChanges(
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        NKikimrSchemeOp::TPersQueueGroupDescription& /*targetConfig*/,
        const NKikimrSchemeOp::TPersQueueGroupDescription& /*sourceConfig*/,
        const bool isCdcStream
    ) override {
        if (isCdcStream) {
            return {Ydb::StatusIds::BAD_REQUEST, "Alter of CDC stream is forbidden"};
        }
        return ApplyAlter(
            modifyScheme,
            Request.settings(),
            GetTopicName(),
            GetTopicName(), // name,
            Database,
            LocalCluster);
    }

    const TString Database;
    const TString LocalCluster;

    Ydb::PersQueue::V1::AlterTopicRequest Request;

};

class TAlterTopicActor: public TGrpcProxyActor<TAlterTopicActor, NKikimr::NGRpcService::TEvPQAlterTopicRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TAlterTopicActor, NKikimr::NGRpcService::TEvPQAlterTopicRequest>;

public:
    TAlterTopicActor(NGRpcService::IRequestOpCtx* request, const TString& localCluster)
        : TGrpcProxyActor<TAlterTopicActor, NKikimr::NGRpcService::TEvPQAlterTopicRequest>(request)
        , LocalCluster(localCluster)
    {
    }

    void DoAction() {
        Become(&TAlterTopicActor::StateWork);

        auto database = CanonizePath(this->Request_->GetDatabaseName().GetOrElse(""));

        Register(NPQ::NSchema::CreateAlterTopicOperationActor(SelfId(), {
            .Database = CanonizePath(this->Request_->GetDatabaseName().GetOrElse("")),
            .PeerName = Request_->GetPeerName(),
            .UserToken = GetUserToken(),
            .Strategy = std::make_unique<TAlterTopicStrategy>(database, std::move(LocalCluster), *GetProtoRequest()),
        }));
    }

private:
    void Handle(NPQ::NSchema::TEvAlterTopicResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ReplyWithError(ev->Get()->Status, ev->Get()->Status, ev->Get()->ErrorMessage);
        } else {
            Ydb::PersQueue::V1::AlterTopicResponse result;
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvAlterTopicResponse, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }

private:
    TString LocalCluster;
};

} // namespace
    
NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request, const TString& localCluster) {
    return new TAlterTopicActor(request, localCluster);
}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1