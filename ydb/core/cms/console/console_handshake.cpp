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
    TConsoleCommitActor(
            TActorId senderId,
            const TString& mainYamlConfig,
            bool allowUnknownFields,
            bool allowIncorrectVersion,
            bool allowIncorrectCluster,
            TActorId interconnectSession,
            ui64 cookie)
        : SenderId(senderId)
        , MainYamlConfig(mainYamlConfig)
        , AllowUnknownFields(allowUnknownFields)
        , AllowIncorrectVersion(allowIncorrectVersion)
        , AllowIncorrectCluster(allowIncorrectCluster)
        , InterconnectSession(interconnectSession)
        , Cookie(cookie)
    {}

    void Bootstrap(const TActorId& consoleId) {
        if (AllowIncorrectVersion xor AllowIncorrectCluster) {
            auto response = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitResponse>();
            response->Record.SetStatus(NKikimrBlobStorage::TEvControllerConsoleCommitResponse::NotCommitted);
            response->Record.SetErrorReason("Options AllowIncorrectVersion and AllowIncorrectCluster currently can be used only together");
            SendInReply(std::move(response));
            PassAway();
        }

        auto executeRequest = [&](auto& request) {
            request->Record.MutableRequest()->set_config(MainYamlConfig);
            request->Record.MutableRequest()->set_allow_unknown_fields(AllowUnknownFields);
            Send(consoleId, request.release());
        };

        if (AllowIncorrectVersion && AllowIncorrectCluster) {
            auto request = std::make_unique<TEvConsole::TEvSetYamlConfigRequest>();
            executeRequest(request);
        } else {
            auto request = std::make_unique<TEvConsole::TEvReplaceYamlConfigRequest>();
            executeRequest(request);
        }

        Become(&TThis::StateWork);
    }

    void Handle(TEvConsole::TEvReplaceYamlConfigResponse::TPtr& /*ev*/) {
        auto response = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitResponse>();
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed);
        SendInReply(std::move(response));
        PassAway();
    }

    void Handle(TEvConsole::TEvSetYamlConfigResponse::TPtr& /*ev*/) {
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
        hFunc(TEvConsole::TEvSetYamlConfigResponse, Handle)
        hFunc(TEvConsole::TEvGenericError, Handle)
        sFunc(TEvents::TEvPoisonPill, PassAway)
    )

private:
    TActorId SenderId;
    TString MainYamlConfig;
    bool AllowUnknownFields;
    bool AllowIncorrectVersion;
    bool AllowIncorrectCluster;
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
    ui64 currentConfigHash = NKikimr::NYaml::GetConfigHash(MainYamlConfig);
    if (Self.CurrentSenderId != ev->Sender) {
        NTabletPipe::CloseServer(Self.SelfId(), Self.CurrentPipeServerId);
    }

    Self.CurrentSenderId = ev->Sender;
    Self.CurrentPipeServerId = ev->Recipient;
    auto response = std::make_unique<TEvBlobStorage::TEvControllerProposeConfigResponse>();
    auto& responseRecord = response->Record;

    if (!record.HasConfigHash()) {
        responseRecord.SetStatus(NKikimrBlobStorage::TEvControllerProposeConfigResponse::ReverseCommit);
        responseRecord.SetConsoleConfigVersion(YamlVersion);
        responseRecord.SetYAML(MainYamlConfig);
    } else if (YamlVersion == proposedConfigVersion) {
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

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerConsoleCommitRequest::TPtr& ev, const TActorContext& /*ctx*/) {
    auto response = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitResponse>();
    auto& record = ev->Get()->Record;
    const auto& mainYamlConfig = record.GetYAML();
    bool allowUnknownFields = record.GetAllowUnknownFields();
    bool allowIncorrectVersion = record.GetAllowIncorrectVersion();
    bool allowIncorrectCluster = record.GetAllowIncorrectCluster();
    if (!CheckSession(*ev, response, NKikimrBlobStorage::TEvControllerConsoleCommitResponse::SessionMismatch)) {
        return;
    }

    IActor* actor = new TConsoleCommitActor(
        ev->Sender,
        mainYamlConfig,
        allowUnknownFields,
        allowIncorrectVersion,
        allowIncorrectCluster,
        ev->InterconnectSession,
        ev->Cookie);
    CommitActor = Register(actor);
}

void TConfigsManager::Handle(TEvBlobStorage::TEvControllerValidateConfigRequest::TPtr& ev, const TActorContext& /*ctx*/) {
    auto response = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigResponse>();
    if (!CheckSession(*ev, response, NKikimrBlobStorage::TEvControllerValidateConfigResponse::IdPipeServerMismatch)) {
        return;
    }

    auto& record = response->Record;

    bool allowIncorrectVersion = ev->Get()->Record.GetAllowIncorrectVersion();
    bool allowIncorrectCluster = ev->Get()->Record.GetAllowIncorrectCluster();

    if (allowIncorrectVersion xor allowIncorrectCluster) {
        record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid);
        record.SetErrorReason("Options AllowIncorrectVersion and AllowIncorrectCluster currently can be used only together");
        SendInReply(ev->Sender, ev->InterconnectSession, std::move(response), ev->Cookie);
        return;
    }

    bool force = allowIncorrectVersion && allowIncorrectCluster;

    auto mainYamlConfig = ev->Get()->Record.GetYAML();

    TUpdateConfigOpContext opCtx;
    ReplaceMainConfigMetadata(mainYamlConfig, force, opCtx);
    ValidateMainConfig(opCtx);
    bool hasForbiddenUnknownFields = !opCtx.UnknownFields.empty() && !ev->Get()->Record.GetAllowUnknownFields();

    if (opCtx.Error || hasForbiddenUnknownFields) {
        record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid);
        TStringStream s;
        if (opCtx.Error) {
            s << *opCtx.Error << (opCtx.UnknownFields.empty() ? "" : " and ");
        }
        if (hasForbiddenUnknownFields) {
            s << "has forbidden unknown fields";
        }
        record.SetErrorReason(s.Str()); // TODO get warnings back
    } else {
        record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid);
        record.SetYAML(opCtx.UpdatedConfig);
    }
    SendInReply(ev->Sender, ev->InterconnectSession, std::move(response), ev->Cookie);
}

}
