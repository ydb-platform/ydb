#include "defs.h"

#include "configs_config.h"
#include "console_impl.h"
#include "console_configs_manager.h"
#include <ydb/public/api/grpc/draft/ydb_dynamic_config_v1.grpc.pb.h>

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NConsole {

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerProposeConfigRequest::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;
    const auto& proposedConfigHash = record.GetConfigHash();

    auto currentConfigHash = std::hash<std::string>{}(YamlConfig);
    if (Self.CurrentSenderId != ev->Sender) {
        auto endSessionEvent = std::make_unique<IEventHandle>(Self.SelfId(), Self.CurrentSenderId, new TEvConsole::TEvCloseSession());
        if (Self.CurrentInterconnectSession) {
            endSessionEvent->Rewrite(TEvInterconnect::EvForward, Self.CurrentInterconnectSession);
            ctx.Send(endSessionEvent.release());
        }
        else {
            return;
        }
    }
    Self.CurrentSenderId = ev->Sender;
    Self.CurrentPipeServerId = ev->Recipient;
    Self.CurrentInterconnectSession = ev->InterconnectSession;
    auto response = std::make_unique<TEvBlobStorage::TEvControllerProposeConfigResponse>();
    auto& responseRecord = response->Record;

    if (YamlVersion > record.GetConfigVersion()) {
        responseRecord.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::UnexpectedConfig);
        responseRecord.SetProposedConfigVersion(proposedConfigVersion);
        responseRecord.SetConsoleConfigVersion(currentConfigVersion);
        SendInReply(ev->Sender, Self.CurrentInterconnectSession, std::move(response));
        LOG_ALERT_S(ctx, NKikimrServices::CMS, "Unexpected proposed config.");
        return;
    }
    if (YamlVersion == record.GetConfigVersion()) {
        responseRecord.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded);
        SendInReply(ev->Sender, Self.CurrentInterconnectSession, std::move(response));
        return;
    }
    if (proposedConfigHash != currentConfigHash) {
        responseRecord.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::HashMismatch);
        responseRecord.SetProposedConfigHash(proposedConfigHash);
        responseRecord.SetConsoleConfigHash(currentConfigHash);
        SendInReply(ev->Sender, Self.CurrentInterconnectSession, std::move(response));
        LOG_ALERT_S(ctx, NKikimrServices::CMS, "Config hash mismatch.");
        return;
    }
    responseRecord.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNeeded);
    SendInReply(ev->Sender, Self.CurrentInterconnectSession, std::move(response));
}

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerConsoleCommitRequest::TPtr &ev, const TActorContext &ctx) {
    auto response = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitResponse>();
    auto& record = response->Record;
    const auto& yamlConfig = ev->Get()->Record.GetYAML();

    if (Self.CurrentSenderId != ev->Sender || Self.CurrentPipeServerId != ev->Recipient) {
        record.SetStatus(NKikimrBlobStorage::TEvControllerConsoleCommitResponse::SessionMismatch);
        SendInReply(ev->Sender, Self.CurrentInterconnectSession, std::move(response));
        return;
    }

    auto replaceConfigEv = TEvConsole::TEvReplaceYamlConfigRequest();
    replaceConfigEv.Record.MutableRequest()->set_config(yamlConfig);
    auto handle = new IEventHandle(ev->Sender, Self.SelfId(), &replaceConfigEv, 0, ev->Cookie);
    ctx.Send(handle);
}

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerValidateConfigRequest::TPtr &ev, const TActorContext &ctx) {
    auto response = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigResponse>();
    auto& record = response->Record;
    auto yamlConfig = ev->Get()->Record.GetYAML();
    if (Self.CurrentSenderId != ev->Sender || Self.CurrentPipeServerId != ev->Recipient) {
        if (Self.CurrentSenderId == ev->Sender) {
            return;
        }
        record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::IdPipeServerMismatch);
        SendInReply(ev->Sender, Self.CurrentInterconnectSession, std::move(response));
        return;
    }
    auto result = ValidateYamlConfig(yamlConfig);
    if (result.ErrorReason || result.HasForbiddenUnknown) {
        record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid);
        SendInReply(ev->Sender, Self.CurrentInterconnectSession, std::move(response));
        return;
    }
    record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid);
    SendInReply(ev->Sender, Self.CurrentInterconnectSession, std::move(response));
}

}