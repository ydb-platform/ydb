#include "defs.h"

#include "configs_config.h"
#include "console_impl.h"
#include "console_configs_manager.h"
#include <ydb/public/api/grpc/draft/ydb_dynamic_config_v1.grpc.pb.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/util/stlog.h>
#include <ydb/core/util/pb.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NConsole {

class TConfigsManager::TConsoleCommitActor : public TActorBootstrapped<TConsoleCommitActor> {
public:
    TConsoleCommitActor(TActorId senderId, const TString& yamlConfig, TActorId interconnectSession, ui64 cookie)
        : SenderId(senderId)
        , YamlConfig(yamlConfig)
        , InterconnectSession(interconnectSession)
        , Cookie(cookie)
    {}

    void Bootstrap(const TActorId& consoleId) {
        auto request = std::make_unique<TEvConsole::TEvReplaceYamlConfigRequest>();
        request->Record.MutableRequest()->set_config(YamlConfig);
        Send(consoleId, request.release());

        Become(&TThis::StateWork);
    }

    void Handle(TEvConsole::TEvReplaceYamlConfigResponse::TPtr& /* ev */) {
        auto response = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitResponse>();
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed);
        SendInReply(std::move(response));
        PassAway();
    }

    void Handle(TEvConsole::TEvGenericError::TPtr& ev) {
        auto response = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitResponse>();
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerConsoleCommitResponse::NotCommitted);
        response->Record.SetErrorReason(SingleLineProto(ev->Get()->Record));
        SendInReply(std::move(response));
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvConsole::TEvReplaceYamlConfigResponse, Handle)
        hFunc(TEvConsole::TEvGenericError, Handle)
        sFunc(TEvents::TEvPoisonPill, PassAway)
    )

private:
    TActorId SenderId;
    TString YamlConfig;
    TActorId InterconnectSession;
    ui64 Cookie;

    void SendInReply(std::unique_ptr<IEventBase> ev) {
        auto h = std::make_unique<IEventHandle>(SenderId, SelfId(), ev.release(), 0, Cookie);
        if (InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, InterconnectSession);
        }
        TActivationContext::Send(h.release());
    }
};

template <typename TRequestEvent, typename TResponse>
bool TConfigsManager::CheckSession(TEventHandle<TRequestEvent>& ev, std::unique_ptr<TResponse>& failEvent, typename TResponse::ProtoRecordType::EStatus status) {
    if (Self.CurrentSenderId != ev.Sender) {
        failEvent->Record.SetStatus(status);
        SendInReply(ev.Sender, ev.InterconnectSession, std::move(failEvent));
        return false;
    } else if (Self.CurrentPipeServerId != ev.Recipient) {
        return false;
    }
    return true;
}

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerProposeConfigRequest::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;
    const auto& proposedConfigHash = record.GetConfigHash();
    const auto& proposedConfigVersion = record.GetConfigVersion();
    ui64 currentConfigHash = NKikimr::NYaml::GetConfigHash(YamlConfig);
    if (Self.CurrentSenderId != ev->Sender) {
        NTabletPipe::CloseServer(Self.SelfId(), Self.CurrentPipeServerId);
    }

    Self.CurrentSenderId = ev->Sender;
    Self.CurrentPipeServerId = ev->Recipient;
    auto response = std::make_unique<TEvBlobStorage::TEvControllerProposeConfigResponse>();
    auto& responseRecord = response->Record;

    if (YamlVersion == proposedConfigVersion) {
        responseRecord.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNeeded);
    } else if (YamlVersion != proposedConfigVersion && (proposedConfigVersion && YamlVersion != proposedConfigVersion - 1)) {
        responseRecord.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::UnexpectedConfig);
        responseRecord.SetProposedConfigVersion(proposedConfigVersion);
        responseRecord.SetConsoleConfigVersion(YamlVersion);
        LOG_ALERT_S(ctx, NKikimrServices::CMS, "Unexpected proposed config.");
    } else if (proposedConfigHash != currentConfigHash) {
        responseRecord.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::HashMismatch);
        responseRecord.SetProposedConfigHash(proposedConfigHash);
        responseRecord.SetConsoleConfigHash(currentConfigHash);
        LOG_ALERT_S(ctx, NKikimrServices::CMS, "Config hash mismatch.");
    } else {
        responseRecord.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded);
    }
    SendInReply(ev->Sender, ev->InterconnectSession, std::move(response), ev->Cookie);
}

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerConsoleCommitRequest::TPtr& ev, const TActorContext& /* ctx */) {
    auto response = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitResponse>();
    const auto& yamlConfig = ev->Get()->Record.GetYAML();
    if (!CheckSession(*ev, response, NKikimrBlobStorage::TEvControllerConsoleCommitResponse::SessionMismatch)) {
        return;
    }

    IActor* actor = new TConsoleCommitActor(ev->Sender, yamlConfig, ev->InterconnectSession, ev->Cookie);
    CommitActor = Register(actor);
}

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerValidateConfigRequest::TPtr& ev, const TActorContext& /* ctx */) {
    auto response = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigResponse>();
    auto requestRecord = ev->Get()->Record;
    response->Record.SetSkipBSCValidation(requestRecord.GetSkipBSCValidation());
    response->Record.SetConfigVersion(requestRecord.GetConfigVersion());
    auto& record = response->Record;
    auto yamlConfig = requestRecord.GetYAML();
    if (!CheckSession(*ev, response, NKikimrBlobStorage::TEvControllerValidateConfigResponse::IdPipeServerMismatch)) {
        return;
    }
    auto result = ValidateConfigAndReplaceMetadata(yamlConfig);
    if (result.ErrorReason || result.HasForbiddenUnknown) {
        record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid);
        if (!result.ErrorReason) {
            record.SetErrorReason("has forbidden unknown fields");
        } else {
            record.SetErrorReason(*result.ErrorReason);
            if (result.HasForbiddenUnknown) {
                record.SetErrorReason(record.GetErrorReason() + " + has forbidden unknown fields");
            }
        }
    } else {
        record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid);
        record.SetYAML(result.UpdatedConfig);
    }
    SendInReply(ev->Sender, ev->InterconnectSession, std::move(response), ev->Cookie);
}

}
