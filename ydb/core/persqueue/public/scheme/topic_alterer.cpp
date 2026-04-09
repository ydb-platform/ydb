#include "topic_alterer.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/ydb_convert/tx_proxy_status.h>

namespace NKikimr::NPQ::NScheme {

TTopicAlterer::TTopicAlterer(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings)
    : TBaseActor<TTopicAlterer>(service)
    , Settings(std::move(settings))
{
}

void TTopicAlterer::Bootstrap() {
    Become(&TTopicAlterer::DescribeState);
    DoDescribe();
}

void TTopicAlterer::PassAway() {
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBaseActor<TTopicAlterer>::PassAway();
}

TString TTopicAlterer::BuildLogPrefix() const {
    return TStringBuilder() << SelfId() << "[" << Settings.Strategy->GetTopicName() << "] ";
}

void TTopicAlterer::DoDescribe() {
    LOG_T("DoDescribe");

    RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
        SelfId(),
        Settings.Database,
        { Settings.Strategy->GetTopicName() },
        {
            .UserToken = Settings.UserToken,
            .AccessRights = NACLib::EAccessRights::AlterSchema
        }));
}

void TTopicAlterer::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    LOG_T("Handle NDescriber::TEvDescribeTopicsResponse");

    auto& topics = ev->Get()->Topics;
    AFL_ENSURE(topics.size() == 1)("s", topics.size());
    
    TopicInfo = std::move(topics.begin()->second);
    switch(TopicInfo.Status) {
        case NDescriber::EStatus::SUCCESS: {
            return DoAlter();
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

    auto workingDir = GetWorkingDir();
    if (workingDir.empty()) {
        return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, "Wrong topic name");
    }

    auto proposal = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();

    proposal->Record.SetDatabaseName(Settings.Database);
    proposal->Record.SetPeerName(Settings.PeerName);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *proposal->Record.MutableTransaction()->MutableModifyScheme();

    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
    modifyScheme.SetWorkingDir(workingDir);
    modifyScheme.SetAllowAccessToPrivatePaths(Settings.IsCdcStreamCompatible);

    auto* config = modifyScheme.MutableAlterPersQueueGroup();
    config->CopyFrom(TopicInfo.Info->Description);

    // keep previous values or set in ModifyPersqueueConfig
    config->ClearTotalGroupCount();
    config->MutablePQTabletConfig()->ClearPartitionKeySchema();

    {
        auto applyIf = modifyScheme.AddApplyIf();
        applyIf->SetPathId(TopicInfo.Self->Info.GetPathId());
        applyIf->SetPathVersion(TopicInfo.Self->Info.GetPathVersion());
    }

    auto [status, error] = Settings.Strategy->ApplyChanges(*config, TopicInfo.Info->Description, true);
    if (status != Ydb::StatusIds::SUCCESS) {
        return ReplyErrorAndDie(status, std::move(error));
    }

    Send(MakeTxProxyID(), std::move(proposal));
}

void TTopicAlterer::Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
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

    ui64 schemeShardTabletId = msg->Record.GetSchemeShardTabletId();
    auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(msg->Record.GetTxId());
    SendToTablet(schemeShardTabletId, request.release());
}

void TTopicAlterer::Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&) {
    Send(Settings.ParentId, new TEvAlterTopicResponse(), 0, Settings.Cookie);
    PassAway();
}

void TTopicAlterer::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    LOG_T("Handle TEvPipeCache::TEvDeliveryProblem");
    return ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Scheme shard " << ev->Get()->TabletId << " is unavailable");
}

STFUNC(TTopicAlterer::AlterState) {
    switch(ev->GetTypeRewrite()) {
        hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
        hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

TString TTopicAlterer::GetWorkingDir() const {
    std::pair <TString, TString> pathPair;
    try {
        pathPair = NKikimr::NGRpcService::SplitPath(TopicInfo.RealPath);
    } catch (const std::exception &ex) {
        return {};
    }
    return pathPair.first;
}

void TTopicAlterer::ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
    LOG_T("ReplyErrorAndDie");
    Send(Settings.ParentId, new TEvErrorResponse(errorCode, std::move(errorMessage)), 0, Settings.Cookie);
    PassAway();
}

void TTopicAlterer::SendToTablet(ui64 tabletId, IEventBase *ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, true, ++Cookie);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
}


IActor* CreateTopicAlterer(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings) {
    return new TTopicAlterer(service, std::move(settings));
}

} // namespace NKikimr::NPQ::NScheme