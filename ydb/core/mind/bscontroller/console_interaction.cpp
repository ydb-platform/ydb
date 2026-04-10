#include "console_interaction.h"
#include "impl.h"

#include <ydb/library/yaml_config/public/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>
#include <ydb/library/yaml_json/yaml_to_json.h>
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
        if (ClientId) {
            IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::SessionClosed, "race");
        }
        GetBlockBackoff.Reset();
        if (ConsolePipe) {
            NTabletPipe::CloseClient(Self.SelfId(), ConsolePipe);
        }
        auto pipe = NTabletPipe::CreateClient(Self.SelfId(), MakeConsoleID(), NTabletPipe::TClientRetryPolicy::WithRetries());
        ConsolePipe = Self.Register(pipe);
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC18, "Console connection service started");
        auto proposeConfigEv = std::make_unique<TEvBlobStorage::TEvControllerProposeConfigRequest>();
        if (Self.YamlConfig) {
            const auto& [yaml, configVersion, yamlReturnedByFetch] = *Self.YamlConfig;
            proposeConfigEv->Record.SetConfigHash(NKikimr::NYaml::GetConfigHash(yamlReturnedByFetch));
            proposeConfigEv->Record.SetConfigVersion(configVersion);
        } else {
            // otherwise Console will send us the most relevant stored YamlConfig
        }
        NTabletPipe::SendData(Self.SelfId(), ConsolePipe, proposeConfigEv.release());
        Working = true;
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvTabletPipe::TEvClientConnected::TPtr& /*ev*/) {
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ConsolePipe != ev->Get()->ClientId) {
            return;
        }
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC33, "Console pipe destroyed", (ConsolePipe, ConsolePipe),
            (ClientId, ev->Get()->ClientId), (Working, Working));
        ConsolePipe = {};
        if (Working) {
            if (ClientId) {
                IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::SessionClosed,
                    "connection to Console tablet terminated");
            }
            MakeGetBlock();
        }
    }

    void TBlobStorageController::TConsoleInteraction::MaybeDisableConfigV2() {
        if (!Self.PendingV2MigrationCheck) {
            return;
        }
        Self.PendingV2MigrationCheck = false;

        // check if switch_to_config_v2 is set in the committed YAML config
        bool switchToConfigV2 = false;
        if (Self.YamlConfig) {
            try {
                const auto& [yaml, configVersion, yamlReturnedByFetch] = *Self.YamlConfig;
                NKikimrConfig::TAppConfig appConfig = NYaml::Parse(yamlReturnedByFetch);
                switchToConfigV2 = appConfig.GetFeatureFlags().GetSwitchToConfigV2();
            } catch (const std::exception& ex) {
                STLOG(PRI_ERROR, BS_CONTROLLER, BSC43, "failed to parse YAML config for V2 migration check",
                    (ErrorReason, ex.what()));
            }
        }

        if (!switchToConfigV2) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSC44, "disabling EnableConfigV2 as SwitchToConfigV2 is not set in YAML config");
            Self.Execute(Self.CreateTxUpdateEnableConfigV2(false));
        }
    }

    void TBlobStorageController::TConsoleInteraction::MakeGetBlock() {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC34, "Issuing GetBlock for BSC");
        auto ev = std::make_unique<TEvBlobStorage::TEvGetBlock>(Self.TabletID(), TInstant::Max());
        auto bsProxyEv = CreateEventForBSProxy(Self.SelfId(), Self.Info()->GroupFor(0, Self.Executor()->Generation()),
            ev.release(), 0);
        TActivationContext::Schedule(GetBlockBackoff.Next(), bsProxyEv);
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerProposeConfigResponse::TPtr &ev) {
        if (!Working) {
            return;
        }

        auto& record = ev->Get()->Record;
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC19, "Console proposed config response", (Response, record));

        if (!Self.EnableConfigV2 && !PendingReplaceRequest) {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSC49, "skipping Console propose response due disabled ConfigV2 and no pending replace",
                (EnableConfigV2, Self.EnableConfigV2), (HasPendingReplaceRequest, static_cast<bool>(PendingReplaceRequest)),
                (Status, record.GetStatus()), (ConsoleConfigVersion, record.GetConsoleConfigVersion()));
            return;
        }

        auto overwriteConfig = [&] {
            TString yamlReturnedByFetch = record.GetYAML();
            if (!yamlReturnedByFetch) {
                MaybeDisableConfigV2();
                return; // no yaml config stored in Console
            }
            try {
                NKikimrConfig::TAppConfig appConfig = NYaml::Parse(yamlReturnedByFetch);
                NKikimrBlobStorage::TStorageConfig storageConfig;
                TString temp;
                if (!NKikimr::NStorage::DeriveStorageConfig(appConfig, &storageConfig, &temp)) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSC21, "failed to derive storage config from one stored in Console",
                        (ErrorReason, temp), (AppConfig, appConfig));
                } else if (auto errorReason = NKikimr::NStorage::ValidateConfig(storageConfig)) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSC23, "failed to validate StorageConfig",
                        (ErrorReason, errorReason), (StorageConfig, storageConfig));
                } else {
                    // try to obtain original config, without version incremented
                    TString yaml;
                    ui64 version = record.GetConsoleConfigVersion() - 1; // Console config version is the next expected one
                    if (auto m = NYamlConfig::GetMainMetadata(yamlReturnedByFetch); m.Version.value_or(0)) {
                        version = m.Version.emplace(*m.Version - 1);
                        yaml = NYamlConfig::ReplaceMetadata(yamlReturnedByFetch, m);
                    }

                    TYamlConfig yamlConfig(std::move(yaml), version, std::move(yamlReturnedByFetch));
                    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC50, "committing config during overwriteConfig");
                    Self.Execute(Self.CreateTxCommitConfig(std::move(yamlConfig), std::nullopt,
                        std::move(storageConfig), std::nullopt, nullptr, std::nullopt,
                        std::nullopt));
                    CommitInProgress = true;
                    MaybeDisableConfigV2();
                }
            } catch (const std::exception& ex) {
                STLOG(PRI_ERROR, BS_CONTROLLER, BSC26, "failed to parse config obtained from Console",
                    (ErrorReason, ex.what()), (Yaml, yamlReturnedByFetch));
            }
        };

        switch (record.GetStatus()) {
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::HashMismatch:
                STLOG(PRI_CRIT, BS_CONTROLLER, BSC25, "Config hash mismatch.");
                Y_DEBUG_ABORT();
                break;

            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::UnexpectedConfig:
                if (record.GetProposedConfigVersion() + 1 < record.GetConsoleConfigVersion()) {
                    // console has a newer config, possibly updated during older version of server running
                    if (Self.StorageYamlConfig) {
                        STLOG(PRI_ERROR, BS_CONTROLLER, BSC30, "Console has newer config, but BSC has dedicated storage"
                            " yaml config section, config not updated");
                    } else if (record.HasYAML()) {
                        overwriteConfig();
                    } else {
                        STLOG(PRI_ERROR, BS_CONTROLLER, BSC32, "Console has newer config, but no yaml was returned");
                    }
                } else {
                    STLOG(PRI_CRIT, BS_CONTROLLER, BSC31, "Console has older config version than BSC");
                    Y_DEBUG_ABORT();
                }
                break;

            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded:
                MaybeDisableConfigV2();
                break;

            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNeeded:
                if (ConsolePipe) {
                    Y_ABORT_UNLESS(Self.YamlConfig);
                    if (const auto& [yaml, configVersion, yamlReturnedByFetch] = *Self.YamlConfig; yaml) {
                        NTabletPipe::SendData(Self.SelfId(), ConsolePipe, new TEvBlobStorage::TEvControllerConsoleCommitRequest(yaml));
                    }
                }
                break;

            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::ReverseCommit:
                STLOG(PRI_DEBUG, BS_CONTROLLER, BSC45, "doing reverse commit");
                if (PendingReplaceRequest) {
                    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC47, "ReverseCommit status received when BSC has PendingReplaceRequest");
                    ExpectedYamlConfigVersion.emplace(record.GetConsoleConfigVersion());
                    Handle(PendingReplaceRequest);
                    PendingReplaceRequest.Reset();
                } else if (!Self.YamlConfig && !Self.StorageYamlConfig) {
                    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC46, "overwriting config");
                    overwriteConfig();
                } else {
                    STLOG(PRI_CRIT, BS_CONTROLLER, BSC29, "ReverseCommit status received when BSC has YamlConfig/StorageYamlConfig",
                        (YamlConfig, Self.YamlConfig), (StorageYamlConfig, Self.StorageYamlConfig), (Record,  record));
                    Y_DEBUG_ABORT();
                }
                break;

            default:
                MakeGetBlock();
                break;
        }
    }

    void TBlobStorageController::TConsoleInteraction::OnConfigCommit() {
        CommitInProgress = false;
        if (!Working) {
            return;
        }
        if (ConsolePipe) {
            Y_ABORT_UNLESS(Self.YamlConfig);
            if (const auto& [yaml, configVersion, yamlReturnedByFetch] = *Self.YamlConfig; yaml) {
                NTabletPipe::SendData(Self.SelfId(), ConsolePipe, new TEvBlobStorage::TEvControllerConsoleCommitRequest(yaml));
            }
        } else {
            Y_ABORT_UNLESS(!ClientId);
        }
    }

    void TBlobStorageController::TConsoleInteraction::Stop() {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC35, "Stopping console interaction", (ConsolePipe, ConsolePipe), (Working, Working));
        if (ConsolePipe) {
            NTabletPipe::CloseClient(Self.SelfId(), ConsolePipe);
            ConsolePipe = {};
        }
        Working = false;
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerConsoleCommitResponse::TPtr &ev) {
        if (!Working) {
            return;
        }
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC20, "Console commit config response", (Response, ev->Get()->Record));
        auto& record = ev->Get()->Record;
        switch (auto status = record.GetStatus()) {
            case NKikimrBlobStorage::TEvControllerConsoleCommitResponse::SessionMismatch:
                NTabletPipe::CloseAndForgetClient(Self.SelfId(), ConsolePipe);
                MakeGetBlock();
                break;

            case NKikimrBlobStorage::TEvControllerConsoleCommitResponse::NotCommitted:
                STLOG(PRI_CRIT, BS_CONTROLLER, BSC28, "Console config not committed");
                Y_DEBUG_ABORT_S(record.GetErrorReason()); // FIXME: fails here on force
                break;

            case NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed:
                MaybeDisableConfigV2();
                if (ClientId) {
                    IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::Success);
                }
                break;

            default:
                Y_FAIL("Unexpected console commit config response status: %d", status);
        }
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerReplaceConfigRequest::TPtr &ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC24, "Console replace config request", (Request, ev->Get()->Record));

        auto& record = ev->Get()->Record;

        const bool reasonOngoingCommit = CommitInProgress || (ClientId && ClientId != ev->Sender);
        if (!ConsolePipe || reasonOngoingCommit || (!Self.EnableConfigV2 && !record.GetSwitchEnableConfigV2())) {
            // reply to newly came query
            const TActorId temp = std::exchange(ClientId, ev->Sender);
            if (!ConsolePipe) {
                IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::SessionClosed,
                    "connection to Console tablet terminated");
            } else if (reasonOngoingCommit) {
                IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::OngoingCommit, "ongoing commit");
            } else {
                IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest, "configuration v2 is disabled", true);
            }
            ClientId = temp;
            return;
        }

        ClientId = ev->Sender;
        ++ExpectedValidationTimeoutCookie;

        // audit log settings
        AuditLogInfo.emplace(record.GetPeerName(), NACLib::TUserToken{record.GetUserToken()});

        if (!Self.ConfigLock.empty() || Self.SelfManagementEnabled) {
            return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::OngoingCommit,
                "configuration is locked by distconf");
        }

        SwitchEnableConfigV2 = record.HasSwitchEnableConfigV2()
            ? std::make_optional(record.GetSwitchEnableConfigV2())
            : std::nullopt;

        if (!Self.EnableConfigV2 && record.HasSwitchDedicatedStorageSection()) {
            return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                "can't enable configuration v2 and switch dedicated storage section mode at the same time");
        }

        if (Self.EnableConfigV2 && SwitchEnableConfigV2 && !*SwitchEnableConfigV2 && Self.StorageYamlConfig) {
            return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                "can't revert to configuration v1 with dedicated storage section enabled");
        }

        if (!Self.EnableConfigV2 && !PendingReplaceRequest) {
            Y_ABORT_UNLESS(SwitchEnableConfigV2);
            Y_ABORT_UNLESS(*SwitchEnableConfigV2);
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSC48, "switching to configuration v2 with ReplaceConfig");
            if (!ConsolePipe) {
                return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::SessionClosed,
                    "connection to Console tablet terminated");
            }

            // just ask console for the latest config
            NTabletPipe::SendData(Self.SelfId(), ConsolePipe, new TEvBlobStorage::TEvControllerProposeConfigRequest);
            PendingReplaceRequest = std::move(ev);
            return;
        }

        PendingStorageYamlConfig.reset();

        if (Self.StorageYamlConfig.has_value()) { // separate configuration
            if (record.HasSwitchDedicatedStorageSection() && !record.GetSwitchDedicatedStorageSection()) {
                // dedicated storage section is being turned off, so we have to have only single one cluster yaml and no storage
                if (record.HasStorageYaml() || record.GetDedicatedConfigMode()) {
                    return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                        "storage YAML is provided while disabling dedicated storage section");
                }
                if (!record.HasClusterYaml()) {
                    // the cluster YAML is to be generated automatically
                    // TODO(alexvru)
                }
                PendingStorageYamlConfig.emplace(std::nullopt);
            } else {
                // we are in separate configuration mode; we can have either one of storage or cluster yamls, or both of them
                if (!record.GetDedicatedConfigMode()) {
                    return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                        "configuration request is submitted as in single-configuration mode");
                }
                if (record.HasStorageYaml()) {
                    PendingStorageYamlConfig.emplace(record.GetStorageYaml());
                }
            }
        } else { // single configuration
            if (record.GetSwitchDedicatedStorageSection()) {
                // turning on separate storage section; the section is either provided or not; in latter case we take it
                // from the cluster yaml somehow
                if (!record.GetDedicatedConfigMode()) {
                    return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                        "configuration request is submitted as in single-configuration mode");
                }
                TString storageYaml;
                if (record.HasStorageYaml()) {
                    storageYaml = record.GetStorageYaml();
                } else {
                    // TODO(alexvru)
                }
                PendingStorageYamlConfig.emplace(storageYaml);
            } else {
                if (record.HasStorageYaml() || record.GetDedicatedConfigMode()) {
                    return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                        "storage YAML is provided when there is no dedicated storage section enabled");
                }
            }
        }

        if (record.HasClusterYaml()) {
            PendingYamlConfig.emplace(record.GetClusterYaml());

            // don't need to reset them explicitly
            // every time we get new request we just replace them
            AllowUnknownFields = record.GetAllowUnknownFields();

            if (Self.YamlConfig && PendingYamlConfig == std::get<0>(*Self.YamlConfig)) {
                PendingYamlConfig.reset(); // no cluster yaml config changed
            }
        } else {
            PendingYamlConfig.reset();
        }

        if (PendingStorageYamlConfig == Self.StorageYamlConfig) {
            PendingStorageYamlConfig.reset(); // no storage yaml config changed
        }

        if (!PendingYamlConfig && !PendingStorageYamlConfig) {
            // nothing changes -- finish request now
            return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::Success);
        }

        if (PendingYamlConfig) {
            const ui64 expected = ExpectedYamlConfigVersion.value_or(Self.YamlConfig ? GetVersion(*Self.YamlConfig) + 1 : 0);

            if (!NYamlConfig::IsMainConfig(*PendingYamlConfig)) {
                return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                    "cluster YAML config is not of MainConfig kind");
            } else if (const auto& meta = NYamlConfig::GetMainMetadata(*PendingYamlConfig); meta.Version != expected) {
                return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                     TStringBuilder() << "cluster YAML config version mismatch got# " << meta.Version
                         << " expected# " << expected);
           } else if (meta.Cluster != AppData()->ClusterName) {
                return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                     TStringBuilder() << "cluster YAML config cluster name mismatch got# " << meta.Cluster
                         << " expected# " << AppData()->ClusterName);
           }
        }

        if (PendingStorageYamlConfig && *PendingStorageYamlConfig) {
            const ui64 expected = Self.ExpectedStorageYamlConfigVersion;

            if (!NYamlConfig::IsStorageConfig(**PendingStorageYamlConfig)) {
                return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                    "storage YAML config is not of StorageConfig kind");
            } else if (const auto& meta = NYamlConfig::GetStorageMetadata(**PendingStorageYamlConfig); meta.Version != expected) {
                return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                    TStringBuilder() << "storage YAML config version mismatch got# " << meta.Version
                        << " expected# " << expected);
           } else if (meta.Cluster != AppData()->ClusterName) {
                return IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest,
                     TStringBuilder() << "storage YAML config cluster name mismatch got# " << meta.Cluster
                         << " expected# " << AppData()->ClusterName);
           }
        }

        if (record.GetSkipConsoleValidation() || !record.HasClusterYaml()) {
            // TODO(alexvru): INCORRECT!!!
            Y_DEBUG_ABORT();
            auto validateConfigResponse = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigResponse>();
            validateConfigResponse->Record.SetStatus(NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid);
            Self.Send(ev->Sender, validateConfigResponse.release());
            return;
        }

        if (!std::exchange(ValidationTimeout, TActivationContext::Now() + TDuration::Minutes(2))) {
            // no timeout event was scheduled
            TActivationContext::Schedule(ValidationTimeout, new IEventHandle(TEvPrivate::EvValidationTimeout, 0,
                Self.SelfId(), TActorId(), nullptr, ExpectedValidationTimeoutCookie));
        }

        auto validateConfigEv = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigRequest>();
        validateConfigEv->Record.SetYAML(record.GetClusterYaml());
        validateConfigEv->Record.SetAllowUnknownFields(record.GetAllowUnknownFields());
        validateConfigEv->Record.SetBypassMetadataChecks(record.GetBypassMetadataChecks());
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC36, "Sending TEvControllerValidateConfigRequest to console",
            (ConsolePipe, ConsolePipe));
        NTabletPipe::SendData(Self.SelfId(), ConsolePipe, validateConfigEv.release());
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerFetchConfigRequest::TPtr &ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC51, "received TEvControllerFetchConfigRequest", (EnableConfigV2, Self.EnableConfigV2));
        const auto& record = ev->Get()->Record;
        auto response = std::make_unique<TEvBlobStorage::TEvControllerFetchConfigResponse>();
        if (!Self.EnableConfigV2) {
            response->Record.SetErrorReason("configuration v2 is disabled");
            response->Record.SetDisabledConfigV2(true);
        } else if (!Self.ConfigLock.empty() || Self.SelfManagementEnabled) {
            response->Record.SetErrorReason("configuration is locked by distconf");
        } else if (Self.StorageYamlConfig) {
            if (record.GetDedicatedStorageSection()) {
                response->Record.SetStorageYaml(*Self.StorageYamlConfig);
            }
            if (record.GetDedicatedClusterSection() && Self.YamlConfig) {
                const auto& [yaml, configVersion, yamlReturnedByFetch] = *Self.YamlConfig;
                response->Record.SetClusterYaml(yaml);
            }
        } else {
            if (!record.GetDedicatedClusterSection() && !record.GetDedicatedStorageSection() && Self.YamlConfig) {
                const auto& [yaml, configVersion, yamlReturnedByFetch] = *Self.YamlConfig;
                response->Record.SetClusterYaml(yaml);
            }
        }
        auto h = std::make_unique<IEventHandle>(ev->Sender, Self.SelfId(), response.release(), 0, ev->Cookie);
        if (ev->InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
        }
        TActivationContext::Send(h.release());
    }

    void TBlobStorageController::TConsoleInteraction::HandleValidationTimeout(TAutoPtr<IEventHandle>& ev) {
        Y_ABORT_UNLESS(ValidationTimeout); // sanity check
        ValidationTimeout = {};

        if (ev->Cookie == ExpectedValidationTimeoutCookie) {
            IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::ValidationTimeout, "validation timeout");
        } else if (ValidationTimeout) {
            TActivationContext::Schedule(ValidationTimeout, new IEventHandle(TEvPrivate::EvValidationTimeout, 0,
                Self.SelfId(), TActorId(), nullptr, ExpectedValidationTimeoutCookie));
        }
    }

    void TBlobStorageController::TConsoleInteraction::CommitConfig() {
        Y_ABORT_UNLESS(PendingCommitState);
        Self.Execute(Self.CreateTxCommitConfig(
            std::move(PendingCommitState->YamlConfig),
            std::exchange(PendingStorageYamlConfig, {}),
            std::move(PendingCommitState->StorageConfig),
            PendingCommitState->ExpectedStorageYamlConfigVersion,
            nullptr,
            SwitchEnableConfigV2,
            std::move(AuditLogInfo)
        ));
        CommitInProgress = true;

        PendingCommitState.reset();
        PendingYamlConfig.reset();
    }

    void TBlobStorageController::TConsoleInteraction::ProcessDryRunResponse(bool success, TString errorReason) {
        if (success) {
            CommitConfig();
        } else {
            IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::InvalidRequest, std::move(errorReason));
        }
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr &ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC27, "Console validate config response", (Response, ev->Get()->Record));
        ++ExpectedValidationTimeoutCookie; // spoil validation timeout cookie to prevent event from firing
        using TResponseProto = NKikimrBlobStorage::TEvControllerReplaceConfigResponse;
        auto& record = ev->Get()->Record;
        switch (auto status = record.GetStatus()) {
            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::IdPipeServerMismatch:
                NTabletPipe::CloseAndForgetClient(Self.SelfId(), ConsolePipe);
                MakeGetBlock();
                return;

            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid:
                return IssueGRpcResponse(TResponseProto::ConsoleInvalidConfig, record.GetErrorReason());

            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid:
                break;

            default:
                Y_FAIL("Unexpected console validate config response status: %d", status);
        }

        struct TExError : std::exception {
            TString ErrorReason;
            TExError(TString errorReason) : ErrorReason(std::move(errorReason)) {}
        };

        try {
            auto parseConfig = [&](const TString& yaml, NKikimrConfig::TAppConfig& appConfig, ui64& version) {
                try {
                    auto json = NYaml::Yaml2Json(YAML::Load(yaml), true);
                    NYaml::Parse(json, NYaml::GetJsonToProtoConfig(AllowUnknownFields), appConfig, true);
                    if (json.Has("metadata")) {
                        if (auto& metadata = json["metadata"]; metadata.Has("version")) {
                            version = metadata["version"].GetUIntegerRobust();
                        }
                    }
                } catch (const std::exception& ex) {
                    throw TExError(TStringBuilder() << "failed to parse YAML config: " << ex.what() << "\n" << yaml);
                }
            };

            const NKikimrConfig::TAppConfig *effectiveConfig = nullptr;

            // parse storage app config, if provided
            std::optional<NKikimrConfig::TAppConfig> storageAppConfig;
            ui64 storageYamlConfigVersion = 0;
            std::optional<ui64> expectedStorageYamlConfigVersion;
            if (PendingStorageYamlConfig && *PendingStorageYamlConfig) {
                parseConfig(**PendingStorageYamlConfig, storageAppConfig.emplace(), storageYamlConfigVersion);
                effectiveConfig = &storageAppConfig.value(); // use this configuration for storage config update
                expectedStorageYamlConfigVersion.emplace(storageYamlConfigVersion + 1); // update expected version
            }

            // parse cluster YAML config, if provided, and calculate its version
            std::optional<NKikimrConfig::TAppConfig> clusterAppConfig;
            ui64 yamlConfigVersion = 0;
            std::optional<TYamlConfig> yamlConfig;
            if (PendingYamlConfig) {
                parseConfig(*PendingYamlConfig, clusterAppConfig.emplace(), yamlConfigVersion);
                if (!effectiveConfig && !Self.StorageYamlConfig) {
                    effectiveConfig = &clusterAppConfig.value(); // use cluster config if we employ single-config mode
                }
                yamlConfig.emplace(std::move(*PendingYamlConfig), yamlConfigVersion, record.GetYAML());
            }

            // check if we are changing StorageConfig here
            std::optional<NKikimrBlobStorage::TStorageConfig> storageConfig;
            if (effectiveConfig) {
                TString temp;
                if (!NKikimr::NStorage::DeriveStorageConfig(*effectiveConfig, &storageConfig.emplace(), &temp)) {
                    throw TExError(TStringBuilder() << "failed to derive AppConfig to StorageConfig: " << temp);
                } else if (auto errorReason = NKikimr::NStorage::ValidateConfig(*storageConfig)) {
                    throw TExError(TStringBuilder() << "failed to validate derived StorageConfig: " << *errorReason);
                }
            }

            PendingCommitState.emplace();
            PendingCommitState->YamlConfig = std::move(yamlConfig);
            PendingCommitState->StorageConfig = std::move(storageConfig);
            PendingCommitState->ExpectedStorageYamlConfigVersion = expectedStorageYamlConfigVersion;

            const ui64 cookie = Self.NextValidationCookie++;
            Self.PendingValidationRequests.emplace(cookie, TConfigValidationInfo{
                .Sender = Self.SelfId(),
                .Cookie = ev->Cookie,
                .InterconnectSession = ev->InterconnectSession,
                .Source = TConfigValidationInfo::ESource::ConsoleInteraction,
            });

            if (PendingCommitState->StorageConfig) {
                auto tempHostRecords = std::make_shared<THostRecordMap::element_type>(*PendingCommitState->StorageConfig);
                if (auto req = Self.BuildConfigRequestFromStorageConfig(*PendingCommitState->StorageConfig, tempHostRecords, true)) {
                    Self.Send(Self.SelfId(), req.release(), 0, cookie);
                } else {
                    Self.PendingValidationRequests.erase(cookie);
                    ProcessDryRunResponse(true);
                }
            } else {
                Self.PendingValidationRequests.erase(cookie);
                ProcessDryRunResponse(true);
            }
        } catch (const TExError& error) {
            IssueGRpcResponse(TResponseProto::BSCInvalidConfig, error.ErrorReason);
        }
    }

    void TBlobStorageController::TConsoleInteraction::Handle(TEvBlobStorage::TEvGetBlockResult::TPtr& ev) {
        auto* msg = ev->Get();

        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC37, "TEvGetBlockResult received", (ConsolePipe, ConsolePipe),
            (Working, Working), (Status, msg->Status), (BlockedGeneration, msg->BlockedGeneration),
            (Generation, Self.Executor()->Generation()));

        if (!Working) {
            return;
        }

        auto blockedGeneration = msg->BlockedGeneration;
        auto generation = Self.Executor()->Generation();
        switch (msg->Status) {
            case NKikimrProto::OK:
                if (generation <= blockedGeneration) {
                    return Self.HandlePoison(TActivationContext::AsActorContext());
                }
                if (generation == blockedGeneration + 1 && !ConsolePipe) {
                    return Start();
                }
                Y_VERIFY_DEBUG_S(generation == blockedGeneration + 1, "BlockedGeneration#" << blockedGeneration
                    << " Tablet generation#" << generation);
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

    void TBlobStorageController::TConsoleInteraction::IssueGRpcResponse(
            NKikimrBlobStorage::TEvControllerReplaceConfigResponse::EStatus status, std::optional<TString> errorReason,
            bool disabledConfigV2) {
        Y_ABORT_UNLESS(ClientId);
        auto resp = std::make_unique<TEvBlobStorage::TEvControllerReplaceConfigResponse>(status, std::move(errorReason));
        if (disabledConfigV2) {
            resp->Record.SetDisabledConfigV2(true);
        }
        Self.Send(ClientId, resp.release());
        ClientId = {};
        ++ExpectedValidationTimeoutCookie; // spoil validation cookie as incoming GRPC request has expired
        PendingYamlConfig.reset();
        PendingStorageYamlConfig.reset();
        ExpectedYamlConfigVersion.reset();
        PendingReplaceRequest.Reset();
    }

}
