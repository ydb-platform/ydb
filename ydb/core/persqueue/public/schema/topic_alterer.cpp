#include "topic_alterer.h"
#include "schema_operation.h"

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

    RegisterWithSameMailbox(CreateSchemaOperation(
        SelfId(),
        TopicInfo.RealPath,
        std::move(proposal),
        Settings.Cookie
    ));
}

void TTopicAlterer::Handle(TEvSchemaOperationResponse::TPtr& ev) {
    LOG_D("Handle TEvSchemaOperationResponse");
    auto& response = *ev->Get();
    if (response.Status != Ydb::StatusIds::SUCCESS) {
        return ReplyErrorAndDie(response.Status, std::move(response.ErrorMessage));
    }
    return ReplyOkAndDie();
}


STFUNC(TTopicAlterer::AlterState) {
    switch(ev->GetTypeRewrite()) {
        hFunc(TEvSchemaOperationResponse, Handle);
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
