#include "topic_alterer.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/ydb_convert/tx_proxy_status.h>

namespace NKikimr::NPQ::NSchema {

TTopicAlterer::TTopicAlterer(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings)
    : TBaseActor<TTopicAlterer>(service)
    , TPipeCacheClient(this)
    , Settings(std::move(settings))
{
}

void TTopicAlterer::Bootstrap() {
    Become(&TTopicAlterer::DescribeState);
    DoDescribe();
}

void TTopicAlterer::PassAway() {
    TPipeCacheClient::Close();
    TBaseActor<TTopicAlterer>::PassAway();
}

void TTopicAlterer::OnException(const std::exception& exc) {
    ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, exc.what());
}

TString TTopicAlterer::BuildLogPrefix() const {
    return TStringBuilder() << SelfId() << "[" << Settings.Strategy->GetTopicName() << "] ";
}

void TTopicAlterer::DoDescribe() {
    LOG_D("DoDescribe");

    RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
        SelfId(),
        Settings.Database,
        { Settings.Strategy->GetTopicName() },
        {
            .UserToken = Settings.UserToken,
            .AccessRights = Settings.Strategy->GetRequiredPermission()
        }));
}

void TTopicAlterer::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

    auto& topics = ev->Get()->Topics;
    AFL_ENSURE(topics.size() == 1)("s", topics.size());
    
    TopicInfo = std::move(topics.begin()->second);
    switch(TopicInfo.Status) {
        case NDescriber::EStatus::SUCCESS: {
            if (TopicInfo.CdcStream && !Settings.Strategy->IsCdcStreamCompatible()) {
                return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, NDescriber::Description(Settings.Strategy->GetTopicName(), NDescriber::EStatus::NOT_FOUND));
            }
            return DoAlter();
        }
        case NDescriber::EStatus::NOT_FOUND: {
            if (Settings.IfExists) {
                return ReplyOkAndDie();
            }
            return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, NDescriber::Description(Settings.Strategy->GetTopicName(), NDescriber::EStatus::NOT_FOUND));
        }
        case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS: {
            return ReplyErrorAndDie(Ydb::StatusIds::UNAUTHORIZED, NDescriber::Description(Settings.Strategy->GetTopicName(), TopicInfo.Status));
        }
        default: {
            return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, NDescriber::Description(Settings.Strategy->GetTopicName(), TopicInfo.Status));
        }
    }
}

STFUNC(TTopicAlterer::DescribeState) {
    switch(ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TTopicAlterer::DoAlter() {
    LOG_D("DoAlter");

    Become(&TTopicAlterer::AlterState);

    auto proposal = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();

    proposal->Record.SetDatabaseName(Settings.Database);
    proposal->Record.SetPeerName(Settings.PeerName);
    if (Settings.UserToken) {
        proposal->Record.SetUserToken(Settings.UserToken->GetSerializedToken());
    }

    auto [status, error] = Settings.Strategy->BuildTransaction(TopicInfo, proposal->Record.MutableTransaction());
    if (status != Ydb::StatusIds::SUCCESS) {
        return ReplyErrorAndDie(status, std::move(error));
    }

    Send(MakeTxProxyID(), std::move(proposal));
}

void TTopicAlterer::Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
    LOG_D("Handle TEvTxUserProxy::TEvProposeTransactionStatus");

    auto msg = ev->Get();
    const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(ev->Get()->Record.GetStatus());

    if (status == TEvTxUserProxy::TResultStatus::ExecError && msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusPreconditionFailed)
    {
        return ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED,
    TStringBuilder() << "Topic with name " << TopicInfo.RealPath << " has another alter in progress");
    }

    Ydb::StatusIds::StatusCode ydbStatus = NKikimr::YdbStatusFromProxyStatus(msg);
    if (!NKikimr::IsTxProxyInProgress(ydbStatus)) {
        TString empty;
        const TString* errorMessage = &empty;
        auto issueMessage = msg->Record.GetIssues();
        if (issueMessage.size() > 0 && issueMessage[0].message().size() > 0) {
            errorMessage = &issueMessage[0].message();
        }
        return ReplyErrorAndDie(ydbStatus, TString{*errorMessage});
    }

    SchemeShardTabletId = msg->Record.GetSchemeShardTabletId();
    TxId = msg->Record.GetTxId();

    DoWaitTxCompletion();
}

void TTopicAlterer::HandleOnAlter(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");
    TPipeCacheClient::OnUndelivered(ev);
    return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Scheme shard " << ev->Get()->TabletId << " is unavailable");
}

STFUNC(TTopicAlterer::AlterState) {
    switch(ev->GetTypeRewrite()) {
        hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
        hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, HandleOnAlter);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TTopicAlterer::DoWaitTxCompletion() {
    LOG_D("DoWaitTxCompletion");
    Become(&TTopicAlterer::WaitTxCompletionState);

    auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(TxId);
    SendToTablet(SchemeShardTabletId, request.release());
}

void TTopicAlterer::Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&) {
    ReplyOkAndDie();
}

void TTopicAlterer::HandleOnWaitTxCompletion(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    LOG_D("Handle TEvPipeCache::TEvDeliveryProblem");
    OnUndelivered(ev);
    if (++WaitTxCompletionRetries > MaxWaitTxCompletionRetries) {
        return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "SchemeShard " << ev->Get()->TabletId << " is unavailable");
    }
    DoWaitTxCompletion();
}

STFUNC(TTopicAlterer::WaitTxCompletionState) {
    switch(ev->GetTypeRewrite()) {
        hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, HandleOnWaitTxCompletion);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TTopicAlterer::ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
    LOG_D("ReplyErrorAndDie: " << errorCode << " " << errorMessage);
    Send(Settings.ParentId, Settings.Strategy->CreateErrorResponse(errorCode, std::move(errorMessage)), 0, Settings.Cookie);
    PassAway();
}

void TTopicAlterer::ReplyOkAndDie() {
    Send(Settings.ParentId, Settings.Strategy->CreateSuccessResponse(), 0, Settings.Cookie);
    PassAway();
}


IActor* CreateTopicAlterer(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings) {
    return new TTopicAlterer(service, std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
