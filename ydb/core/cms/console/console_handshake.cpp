#include "defs.h"

#include "configs_config.h"
#include "console.h"

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NConsole {

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerProposeConfigRequest::TPtr &ev, const TActorContext &ctx) {
    const auto& proposedConfigHash = ev->Get()->Record.GetConfigHash();

    auto currentConfigHash = std::hash<std::string>{}(YamlConfig);
    if (currentSenderId != ev->Sender) {
        auto endSessionEvent = std::make_unique<IEventHandle>(SelfId(), currentSenderId, new TEvConsole::TEvCloseSession());
        if (currentInterconnectSession) {
            endSessionEvent->Rewrite(TEvInterconnect::EvForward, currentInterconnectSession);
        }
        ctx.Send(endSessionEvent.release());
    }
    currentSenderId = ev->Sender;
    currentPipeServerId = ev->Recipient;
    currentInterconnectSession = ev.InterconnectSession;
    auto response = std::make_unique<TEvBlobStorage::TEvControllerProposeConfigResponse>();

    if (consoleVersion > ev->Record.GetVersion()) {
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::UnexpectedConfig);
        response->Record.SetProposedConfigHash(proposedConfigHash);
        response->Record.SetConsoleConfigHash(currentConfigHash);
        ctx.Send(ev->Sender, response.release());
        LOG_ALERT_S(ctx, NKikimrServices::CMS, "Unexpected proposed config.");
        return;
    }
    if (consoleVersion == ev->Record.GetVersion()) {
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded);
        ctx.Send(ev->Sender, response.release());
        return;
    }
    if (proposedConfigHash != currentConfigHash) {
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::HashMismatch);
        response->Record.SetProposedConfigHash(proposedConfigHash);
        response->Record.SetConsoleConfigHash(currentConfigHash);
        ctx.Send(ev->Sender, response.release());
        LOG_ALERT_S(ctx, NKikimrServices::CMS, "Config hash mismatch.");
        return;
    }
    response->Record.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNeeded);
    ctx.Send(ev->Sender, response.release());
}

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerCommitConfigRequest::TPtr &ev, const TActorContext &ctx) {
    auto response = std::make_unique<TEvBlobStorage::TEvControllerCommitConfigResponse>();

    if (currentSenderId != ev->Sender || currentPipeServerId != ev->Recipient) {
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerCommitConfigResponse::SessionMismatch);
        ctx.Send(ev->Sender, response.release());
        return;
    }

    auto replaceConfigEv = std::make_unique<TEvConsole::TEvReplaceConfigYamlRequest>();
    auto dynamicConfigRequest = std::make_unique<Ydb::DynamicConfig::V1::ReplaceDynamicConfigRequest>();
    dynamicConfigRequest->set_yaml_config(ev->Record.GetYAML());
    replaceConfigEv->Record.SetRequest(dynamicConfigRequest.release());

    auto tx = std::make_unique<TTxReplaceConfigYaml>(this, replaceConfigEv.release());
    if (!tx->Execute(ctx)) {
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerCommitConfigResponse::NotCommited);
        ctx.Send(ev->Sender, response.release());
        return;
    }
    tx->Complete(ctx);
    response->Record.SetStatus(NKikimrBlobStorage::TEvControllerCommitConfigResponse::Commited);
    ctx.Send(ev->Sender, response.release());
}

}