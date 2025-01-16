#include "console_interaction.h"
#include "impl.h"

#include <ydb/library/yaml_config/yaml_config_helpers.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>

namespace NKikimr::NBsController {

    void TBlobStorageController::StartConsoleInteraction() {
        ConsoleInteraction = std::make_unique<TConsoleInteraction>(*this);
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC22, "Console interaction started");
    }

    TBlobStorageController::TConsoleInteraction::TConsoleInteraction(TBlobStorageController& controller)
        : Self(controller)
    {}

    void TBlobStorageController::TConsoleInteraction::Start() {
        if (GRPCSenderId) {
            auto response = std::make_unique<TEvBlobStorage::TEvControllerReplaceConfigResponse>();
            response->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::SessionClosed);
            Self.Send(GRPCSenderId, response.release());
            GRPCSenderId = {};
        }
        GetBlockBackoff.Reset();
        if (ConsolePipe) {
            NTabletPipe::CloseClient(Self.SelfId(), ConsolePipe);
        }
        auto pipe = NTabletPipe::CreateClient(Self.SelfId(), MakeConsoleID(), NTabletPipe::TClientRetryPolicy::WithRetries());
        ConsolePipe = Self.Register(pipe);
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC18, "Console connection service started");
        auto proposeConfigEv = MakeHolder<TEvBlobStorage::TEvControllerProposeConfigRequest>();
        auto configHash = NKikimr::NYaml::GetConfigHash(Self.YamlConfig);
        proposeConfigEv->Record.SetConfigHash(configHash);
        proposeConfigEv->Record.SetConfigVersion(Self.ConfigVersion);
        NTabletPipe::SendData(Self.SelfId(), ConsolePipe, proposeConfigEv.Release());
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvTabletPipe::TEvClientConnected::TPtr& /*ev*/) {
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& /*ev*/) {
        ConsolePipe = {};
        MakeGetBlock();
    }

    void TBlobStorageController::TConsoleInteraction::MakeCommitToConsole(TString& /* config */, ui32 /* configVersion */) {
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerConsoleCommitRequest>();
        ev->Record.SetYAML(Self.YamlConfig);
        NTabletPipe::SendData(Self.SelfId(), ConsolePipe, ev.Release());
    }

    void TBlobStorageController::TConsoleInteraction::MakeGetBlock() {
        auto ev = MakeHolder<TEvBlobStorage::TEvGetBlock>(Self.TabletID(), TInstant::Max());
        auto bsProxyEv = CreateEventForBSProxy(Self.SelfId(), Self.Info()->GroupFor(0, Self.Executor()->Generation()), ev.Release(), 0);
        TActivationContext::Schedule(TDuration::MilliSeconds(GetBlockBackoff.NextBackoffMs()), bsProxyEv);
    }

    void TBlobStorageController::TConsoleInteraction::MakeRetrySession() {
        NeedRetrySession = false;
        Start();
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerProposeConfigResponse::TPtr &ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC19, "Console proposed config response", (Response, ev->Get()->Record));
        auto& record = ev->Get()->Record;
        auto status = record.GetStatus();
        auto commitConfigEv = MakeHolder<TEvBlobStorage::TEvControllerConsoleCommitRequest>();
        switch(status) {
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::HashMismatch:
                STLOG(PRI_ALERT, BS_CONTROLLER, BSC25, "Config hash mismatch.");
                return;
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::UnexpectedConfig:
                MakeGetBlock();
                return;
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded:
                return;
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNeeded:
                commitConfigEv->Record.SetYAML(Self.YamlConfig);
                commitConfigEv->Record.SetConfigVersion(Self.ConfigVersion);
                Self.Send(Self.SelfId(), commitConfigEv.Release());
                return;
            default:
                MakeGetBlock();
                return;
        }
    }

    void TBlobStorageController::TConsoleInteraction::OnConfigCommit(const TCommitConfigResult& result) {
        if (result.Status == TCommitConfigResult::EStatus::ParseError ||
                result.Status == TCommitConfigResult::EStatus::ValidationError) {
            auto response = std::make_unique<TEvBlobStorage::TEvControllerReplaceConfigResponse>();
            response->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::BSCInvalidConfig);
            Self.Send(GRPCSenderId, response.release());
            GRPCSenderId = {};
            return;
        }
        if (result.Status == TCommitConfigResult::EStatus::Success) {
            auto commitConfigEv = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitRequest>();
            commitConfigEv->Record.SetYAML(Self.YamlConfig);
            commitConfigEv->Record.SetConfigVersion(Self.ConfigVersion);
            NTabletPipe::SendData(Self.SelfId(), ConsolePipe, commitConfigEv.release());
            return;
        }
    }

    void TBlobStorageController::TConsoleInteraction::OnPassAway() {
        if (ConsolePipe) {
            NTabletPipe::CloseClient(Self.SelfId(), ConsolePipe);
        }
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerConsoleCommitResponse::TPtr &ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC20, "Console commit config response", (Response, ev->Get()->Record));
        auto& record = ev->Get()->Record;
        auto status = record.GetStatus();
        switch(status) {
            case NKikimrBlobStorage::TEvControllerConsoleCommitResponse::SessionMismatch:
                MakeGetBlock();
                NeedRetrySession = true;
                return;
            case NKikimrBlobStorage::TEvControllerConsoleCommitResponse::NotCommitted:
                STLOG(PRI_ALERT, BS_CONTROLLER, BSC28, "Console commit config not commited");
                return;
            case NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed:
                if (GRPCSenderId) {
                    auto response = std::make_unique<TEvBlobStorage::TEvControllerReplaceConfigResponse>();
                    response->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::Success);
                    Self.Send(GRPCSenderId, response.release());
                }
                return;
            default:
                Y_FAIL("Unexpected console commit config response status: %d", status);
        }
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerReplaceConfigRequest::TPtr &ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC24, "Console replace config request", (Request, ev->Get()->Record));
        auto& record = ev->Get()->Record;
        auto response = MakeHolder<TEvBlobStorage::TEvControllerReplaceConfigResponse>();
        if (!record.GetOverwriteFlag() && GRPCSenderId) {
            response->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::OngoingCommit);
            Self.Send(ev->Sender, response.Release());
            return;
        }
        GRPCSenderId = ev->Sender;
        if (!record.GetSkipConsoleValidation()) {
            if (!ValidationTimeout) {
                TActivationContext::Schedule(TDuration::Minutes(2), new IEventHandle(Self.SelfId(), Self.SelfId(),
                    new TEvPrivate::TEvValidationTimeout()));
            }
            ValidationTimeout = TActivationContext::Now() + TDuration::Minutes(2);
            auto validateConfigEv = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigRequest>();
            validateConfigEv->Record.SetYAML(record.GetYAML());
            validateConfigEv->Record.SetConfigVersion(record.GetConfigVersion());
            validateConfigEv->Record.SetSkipBSCValidation(record.GetSkipBSCValidation());
            NTabletPipe::SendData(Self.SelfId(), ConsolePipe, validateConfigEv.release());
        }
        auto validateConfigResponse = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigResponse>();
        validateConfigResponse->Record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid);
        Self.Send(ev->Sender, validateConfigResponse.release());
    }

    bool TBlobStorageController::TConsoleInteraction::ParseConfig(const TString& config, ui32& /*configVersion*/,
            NKikimrBlobStorage::TStorageConfig& storageConfig) {
        NKikimrConfig::TAppConfig appConfig;
        try {
            appConfig = NKikimr::NYaml::Parse(config);
        } catch (const std::exception& ex) {
            return false;
        }
        const bool success = NKikimr::NStorage::DeriveStorageConfig(appConfig, &storageConfig, nullptr);
        return success;
    }

    TBlobStorageController::TConsoleInteraction::TCommitConfigResult::EStatus
            TBlobStorageController::TConsoleInteraction::CheckConfig(const TString& config, ui32& configVersion,
            bool skipBSCValidation, NKikimrBlobStorage::TStorageConfig& storageConfig) {
        if (!ParseConfig(config, configVersion, storageConfig)) {
            return TConsoleInteraction::TCommitConfigResult::EStatus::ParseError;
        }
        if (!skipBSCValidation) {
            auto ErrorReason = NKikimr::NStorage::ValidateConfig(storageConfig);
            if (ErrorReason) {
                return TConsoleInteraction::TCommitConfigResult::EStatus::ValidationError;
            }
        }
        return TConsoleInteraction::TCommitConfigResult::EStatus::Success;
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TBlobStorageController::TConsoleInteraction::TEvPrivate::TEvValidationTimeout::TPtr& /* ev */) {
        if (TActivationContext::Now() < ValidationTimeout) {
            return;
        }
        ValidationTimeout = {};
        auto response = MakeHolder<TEvBlobStorage::TEvControllerReplaceConfigResponse>();
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::ValidationTimeout);
        Self.Send(GRPCSenderId, response.Release());
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr &ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC27, "Console validate config response", (Response, ev->Get()->Record));
        ValidationTimeout = {};
        auto& record = ev->Get()->Record;
        auto status = record.GetStatus();
        auto replaceConfigResponse = std::make_unique<TEvBlobStorage::TEvControllerReplaceConfigResponse>();
        switch(status) {
            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::IdPipeServerMismatch:
                MakeGetBlock();
                NeedRetrySession = true;
                return;
            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid:
                replaceConfigResponse->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::ConsoleInvalidConfig);
                Self.Send(GRPCSenderId, replaceConfigResponse.release());
                return;
            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid:
                break;
            default:
                Y_FAIL("Unexpected console validate config response status: %d", status);
        }
        NKikimrBlobStorage::TStorageConfig StorageConfig;
        TString yamlConfig = record.GetYAML();
        ui32 configVersion = record.GetConfigVersion();
        if (CheckConfig(yamlConfig, configVersion, record.GetSkipBSCValidation(), StorageConfig) ==
                TConsoleInteraction::TCommitConfigResult::EStatus::Success) {
            Self.Execute(Self.CreateTxCommitConfig(yamlConfig, configVersion, StorageConfig));
            return;
        }
        replaceConfigResponse->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::BSCInvalidConfig);
        Self.Send(GRPCSenderId, replaceConfigResponse.release());
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvGetBlockResult::TPtr& ev) {
        auto* msg = ev->Get();
        auto status = msg->Status;
        auto blockedGeneration = msg->BlockedGeneration;
        auto generation = Self.Executor()->Generation();
        switch (status) {
            case NKikimrProto::OK:
                if (generation <= blockedGeneration) {
                    Self.HandlePoison(TActivationContext::AsActorContext());
                    return;
                }
                if (generation == blockedGeneration + 1 && NeedRetrySession) {
                    MakeRetrySession();
                    return;
                }
                Y_VERIFY_DEBUG_S(generation == blockedGeneration + 1, "BlockedGeneration#" << blockedGeneration << " Tablet generation#" << generation);
                break;
            case NKikimrProto::BLOCKED:
                Self.HandlePoison(TActivationContext::AsActorContext());
                break;
            case NKikimrProto::DEADLINE:
            case NKikimrProto::RACE:
            case NKikimrProto::ERROR:
                MakeGetBlock();
                break;
            default:
                Y_ABORT("unexpected status");
        }
    }
}
