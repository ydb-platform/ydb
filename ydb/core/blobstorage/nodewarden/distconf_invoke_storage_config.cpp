#include "distconf_invoke.h"
#include "node_warden_impl.h"

#include <ydb/core/mind/bscontroller/bsc.h>

#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_json/yaml_to_json.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::FetchStorageConfig(bool manual, bool fetchMain, bool fetchStorage) {
        if (!Self->StorageConfig) {
            FinishWithError(TResult::ERROR, "no agreed StorageConfig");
        } else if (!Self->MainConfigFetchYaml) {
            FinishWithError(TResult::ERROR, "no stored YAML for storage config");
        } else {
            auto ev = PrepareResult(TResult::OK, std::nullopt);
            auto *record = &ev->Record;
            auto *res = record->MutableFetchStorageConfig();
            if (fetchMain) {
                res->SetYAML(Self->MainConfigFetchYaml);
            }
            if (fetchStorage && Self->StorageConfigYaml) {
                auto metadata = NYamlConfig::GetStorageMetadata(*Self->StorageConfigYaml);
                metadata.Cluster = metadata.Cluster.value_or("unknown"); // TODO: fix this
                metadata.Version = metadata.Version.value_or(0) + 1;
                res->SetStorageYAML(NYamlConfig::ReplaceMetadata(*Self->StorageConfigYaml, metadata));
            }

            if (manual) {
                // add BlobStorageConfig, NameserviceConfig, DomainsConfig into main/storage config
            }

            Finish(Sender, SelfId(), ev.release(), 0, Cookie);
        }
    }

    void TInvokeRequestHandlerActor::ReplaceStorageConfig(const TQuery::TReplaceStorageConfig& request) {
        if (!RunCommonChecks()) {
            return;
        } else if (!Self->ConfigCommittedToConsole && Self->SelfManagementEnabled) {
            return FinishWithError(TResult::ERROR, "previous config has not been committed to Console yet");
        }

        // extract YAML files provided by the user
        NewYaml = request.HasYAML() ? std::make_optional(request.GetYAML()) : std::nullopt;
        NewStorageYaml = request.HasStorageYAML() ? std::make_optional(request.GetStorageYAML()) : std::nullopt;

        // start deriving a config from current one
        TString state;
        NKikimrBlobStorage::TStorageConfig config(*Self->StorageConfig);

        try {
            auto load = [&](const TString& yaml, ui64& version, const char *expectedKind) {
                state = TStringBuilder() << "loading " << expectedKind << " YAML";
                NJson::TJsonValue json = NYaml::Yaml2Json(YAML::Load(yaml), true);

                state = TStringBuilder() << "extracting " << expectedKind << " metadata";
                if (!json.Has("metadata") || !json["metadata"].IsMap()) {
                    throw yexception() << "no metadata section";
                }
                auto& metadata = json["metadata"];
                NYaml::ValidateMetadata(metadata);
                if (!metadata.Has("kind") || metadata["kind"] != expectedKind) {
                    throw yexception() << "missing or invalid kind provided";
                }
                version = metadata["version"].GetUIntegerRobust();

                state = TStringBuilder() << "validating " << expectedKind << " config section";
                if (!json.Has("config") || !json["config"].IsMap()) {
                    throw yexception() << "missing config section";
                }

                return json;
            };

            NJson::TJsonValue main;
            NJson::TJsonValue storage;
            const NJson::TJsonValue *effective = nullptr;

            if (NewStorageYaml) {
                storage = load(*NewStorageYaml, StorageYamlVersion.emplace(), "StorageConfig");
                config.SetExpectedStorageYamlVersion(*StorageYamlVersion + 1);
                effective = &storage;
            }

            if (NewYaml) {
                main = load(*NewYaml, MainYamlVersion.emplace(), "MainConfig");
                if (!effective && !request.GetDedicatedStorageSectionConfigMode()) {
                    // we will parse main config as distconf's one, as the user is not expecting us to have two
                    // separate configs; we'll check if this is correct later
                    effective = &main;
                }
            }

            if (effective) {
                state = "parsing final config";

                NKikimrConfig::TAppConfig appConfig;
                NYaml::Parse(*effective, NYaml::GetJsonToProtoConfig(), appConfig, true);

                if (TString errorReason; !DeriveStorageConfig(appConfig, &config, &errorReason)) {
                    return FinishWithError(TResult::ERROR, TStringBuilder()
                        << "error while deriving StorageConfig: " << errorReason);
                }
            }
        } catch (const std::exception& ex) {
             return FinishWithError(TResult::ERROR, TStringBuilder() << "exception while " << state
                << ": " << ex.what());
        }

        // advance the config generation
        config.SetGeneration(config.GetGeneration() + 1);

        // make it proposed one
        ProposedStorageConfig = std::move(config);

        // check if we are enabling distconf by this operation and handle it accordingly
        if (!Self->SelfManagementEnabled && ProposedStorageConfig.GetSelfManagementConfig().GetEnabled()) {
            ControllerOp = EControllerOp::ENABLE_DISTCONF;
            TryEnableDistconf(); // collect quorum of configs first to see if we have to do rolling restart of the cluster
        } else {
            if (Self->SelfManagementEnabled && !ProposedStorageConfig.GetSelfManagementConfig().GetEnabled()) {
                ControllerOp = EControllerOp::DISABLE_DISTCONF;
            } else {
                ControllerOp = EControllerOp::OTHER;
            }
            ConnectToController();
        }
    }

    void TInvokeRequestHandlerActor::ReplaceStorageConfigResume(const std::optional<TString>& storageConfigYaml, ui64 expectedMainYamlVersion,
            ui64 expectedStorageYamlVersion, bool enablingDistconf) {
        const auto& request = Event->Get()->Record.GetReplaceStorageConfig();

        auto switchDedicatedStorageSection = request.HasSwitchDedicatedStorageSection()
            ? std::make_optional(request.GetSwitchDedicatedStorageSection())
            : std::nullopt;

        const bool targetDedicatedStorageSection = switchDedicatedStorageSection.value_or(storageConfigYaml.has_value());

        if (switchDedicatedStorageSection) {
            // check that configs are explicitly defined when we are switching dual-config mode
            if (!NewYaml) {
                return FinishWithError(TResult::ERROR, "main config must be specified when switching dedicated"
                    " storage section mode");
            } else if (*switchDedicatedStorageSection && !NewStorageYaml) {
                return FinishWithError(TResult::ERROR, "storage config must be specified when turning on dedicated"
                    " storage section mode");
            }
        }

        if (request.GetDedicatedStorageSectionConfigMode() != targetDedicatedStorageSection) {
            return FinishWithError(TResult::ERROR, "DedicatedStorageSectionConfigMode does not match target state");
        } else if (NewStorageYaml && !targetDedicatedStorageSection) {
            // we are going to end up in single-config mode, but explicit storage yaml is provided
            return FinishWithError(TResult::ERROR, "unexpected dedicated storage config section in request");
        } else if (switchDedicatedStorageSection && *switchDedicatedStorageSection == storageConfigYaml.has_value()) {
            // this enable/disable command does not change the state
            return FinishWithError(TResult::ERROR, "dedicated storage config section is already in requested state");
        }

        if (StorageYamlVersion && *StorageYamlVersion != expectedStorageYamlVersion) {
            return FinishWithError(TResult::ERROR, TStringBuilder()
                << "storage config version must be increasing by one"
                << " new version# " << *StorageYamlVersion
                << " expected version# " << expectedStorageYamlVersion);
        }

        if (MainYamlVersion && *MainYamlVersion != expectedMainYamlVersion) {
            return FinishWithError(TResult::ERROR, TStringBuilder()
                << "main config version must be increasing by one"
                << " new version# " << *MainYamlVersion
                << " expected version# " << expectedMainYamlVersion);
        }

        if (auto error = ValidateConfig(*Self->StorageConfig)) {
            return FinishWithError(TResult::ERROR, TStringBuilder()
                << "ReplaceStorageConfig current config validation failed: " << *error);
        } else if (auto error = ValidateConfigUpdate(*Self->StorageConfig, ProposedStorageConfig)) {
            return FinishWithError(TResult::ERROR, TStringBuilder()
                << "ReplaceStorageConfig config validation failed: " << *error);
        }

        // update main config yaml in the StorageConfig
        if (NewYaml) {
            if (const auto& error = UpdateConfigComposite(ProposedStorageConfig, *NewYaml, std::nullopt)) {
                return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to update config yaml: " << *error);
            }
        }

        // do the same thing for storage config yaml
        if (NewStorageYaml) {
            TString s;
            if (TStringOutput output(s); true) {
                TZstdCompress zstd(&output);
                zstd << *NewStorageYaml;
            }
            ProposedStorageConfig.SetCompressedStorageYaml(s);
        } else if (!targetDedicatedStorageSection) {
            ProposedStorageConfig.ClearCompressedStorageYaml();
        }

        if (request.GetSkipConsoleValidation() || !NewYaml) {
            StartProposition(&ProposedStorageConfig);
        } else if (!Self->EnqueueConsoleConfigValidation(SelfId(), enablingDistconf, *NewYaml)) {
            FinishWithError(TResult::ERROR, "console pipe is not available");
        }
    }

    void TInvokeRequestHandlerActor::TryEnableDistconf() {
        const ERootState prevState = std::exchange(Self->RootState, ERootState::IN_PROGRESS);
        Y_ABORT_UNLESS(prevState == ERootState::RELAX);

        TEvScatter task;
        task.MutableCollectConfigs();
        IssueScatterTask(std::move(task), [this](TEvGather *res) -> std::optional<TString> {
            Y_ABORT_UNLESS(Self->StorageConfig); // it can't just disappear

            const ERootState prevState = std::exchange(Self->RootState, ERootState::RELAX);
            Y_ABORT_UNLESS(prevState == ERootState::IN_PROGRESS);

            if (!res->HasCollectConfigs()) {
                return "incorrect CollectConfigs response";
            } else if (Self->CurrentProposedStorageConfig) {
                FinishWithError(TResult::RACE, "config proposition request in flight");
            } else if (Scepter.expired()) {
                return "scepter lost during query execution";
            } else {
                auto r = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), std::nullopt);
                return std::visit<std::optional<TString>>(TOverloaded{
                    [&](std::monostate&) -> std::optional<TString> {
                        if (r.IsDistconfDisabledQuorum) {
                            // distconf is disabled on the majority of nodes; we have just to replace configs
                            // and then to restart these nodes in order to enable it in future
                            auto ev = PrepareResult(TResult::CONTINUE_BSC, "proceed with BSC");
                            ev->Record.MutableReplaceStorageConfig()->SetAllowEnablingDistconf(true);
                            Finish(Sender, SelfId(), ev.release(), 0, Cookie);
                        } else {
                            ConnectToController();
                        }
                        return std::nullopt;
                    },
                    [&](TString& error) {
                        return std::move(error);
                    },
                    [&](NKikimrBlobStorage::TStorageConfig& /*proposedConfig*/) {
                        return "unexpected config proposition";
                    }
                }, r.Outcome);
            }

            return std::nullopt; // no error or it is already processed
        });
    }

    void TInvokeRequestHandlerActor::ConnectToController() {
        ControllerPipeId = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID(),
            NTabletPipe::TClientRetryPolicy::WithRetries()));
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->AddCommand()->MutableGetInterfaceVersion();
        NTabletPipe::SendData(SelfId(), ControllerPipeId, ev.release());
    }

    void TInvokeRequestHandlerActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_NODE, NWDC65, "received TEvClientConnected", (SelfId, SelfId()), (Status, msg.Status),
            (ClientId, msg.ClientId), (ServerId, msg.ServerId));

        if (msg.Status != NKikimrProto::OK) {
            ControllerPipeId = {};
            return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to connect to BSC with " << msg.Status);
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_NODE, NWDC79, "received TEvClientDestroyed", (SelfId, SelfId()),
            (ClientId, msg.ClientId), (ServerId, msg.ServerId));

        ControllerPipeId = {};
        FinishWithError(TResult::ERROR, "pipe to BSC disconnected");
    }

    void TInvokeRequestHandlerActor::Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev) {
        const auto& response = ev->Get()->Record.GetResponse();
        STLOG(PRI_DEBUG, BS_NODE, NWDC80, "received TEvControllerConfigResponse", (SelfId, SelfId()),
            (Response, response));
        if (response.StatusSize() != 1 || response.GetStatus(0).GetInterfaceVersion() < BSC_INTERFACE_DISTCONF_CONTROL) {
            return FinishWithError(TResult::ERROR, "BSC controller is way too old to process this query");
        }

        auto request = std::make_unique<TEvBlobStorage::TEvControllerDistconfRequest>();
        auto& record = request->Record;
        const auto& replaceStorageConfig = Event->Get()->Record.GetReplaceStorageConfig();

        // provide the full main config to the recipient (in case when user changes it, or when we are managing it)
        if (NewYaml) {
            record.SetCompressedMainConfig(NYamlConfig::CompressYamlString(*NewYaml));
        } else if (Self->SelfManagementEnabled) {
            record.SetCompressedMainConfig(NYamlConfig::CompressYamlString(Self->MainConfigYaml));
        }

        // do the same thing to the storage config
        if (NewStorageYaml) {
            record.SetCompressedStorageConfig(NYamlConfig::CompressYamlString(*NewStorageYaml));
        } else if (Self->SelfManagementEnabled && Self->StorageConfigYaml) {
            record.SetCompressedStorageConfig(NYamlConfig::CompressYamlString(*Self->StorageConfigYaml));
        }

        record.SetDedicatedConfigMode(replaceStorageConfig.GetDedicatedStorageSectionConfigMode());

        // fill in operation and operation-dependent fields
        switch (ControllerOp) {
            case EControllerOp::ENABLE_DISTCONF:
                record.SetOperation(NKikimrBlobStorage::TEvControllerDistconfRequest::EnableDistconf);
                break;

            case EControllerOp::DISABLE_DISTCONF:
                record.SetOperation(NKikimrBlobStorage::TEvControllerDistconfRequest::DisableDistconf);
                if (ProposedStorageConfig.HasExpectedStorageYamlVersion()) {
                    record.SetExpectedStorageConfigVersion(ProposedStorageConfig.GetExpectedStorageYamlVersion());
                }
                break;

            case EControllerOp::OTHER:
                record.SetOperation(NKikimrBlobStorage::TEvControllerDistconfRequest::ValidateConfig);
                break;

            case EControllerOp::UNSET:
                Y_DEBUG_ABORT();
        }

        NTabletPipe::SendData(SelfId(), ControllerPipeId, request.release());
    }

    void TInvokeRequestHandlerActor::Handle(TEvBlobStorage::TEvControllerDistconfResponse::TPtr ev) {
        auto& record = ev->Get()->Record;

        std::optional<TString> mainYaml;
        std::optional<TString> storageYaml;

        if (record.HasCompressedMainConfig()) {
            mainYaml = NYamlConfig::DecompressYamlString(record.GetCompressedMainConfig());
        }
        if (record.HasCompressedStorageConfig()) {
            storageYaml = NYamlConfig::DecompressYamlString(record.GetCompressedStorageConfig());
        }

        auto getRecord = [&] {
            if (mainYaml) {
                record.SetCompressedMainConfig(*mainYaml);
            }
            if (storageYaml) {
                record.SetCompressedStorageConfig(*storageYaml);
            }
            return record;
        };

        STLOG(PRI_DEBUG, BS_NODE, NWDC81, "received TEvControllerDistconfResponse", (SelfId, SelfId()),
            (Record, getRecord()));

        if (const auto& status = record.GetStatus(); status != NKikimrBlobStorage::TEvControllerDistconfResponse::OK) {
            return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to interact with BSC to update configuration"
                << " Status# " << NKikimrBlobStorage::TEvControllerDistconfResponse::EStatus_Name(status)
                << " ErrorReason# " << record.GetErrorReason());
        }

        if (ControllerOp == EControllerOp::ENABLE_DISTCONF && !NewYaml) {
            // in case we are enabling distconf through dedicated storage section
            NewYaml = std::move(mainYaml);
        }

        switch (ControllerOp) {
            case EControllerOp::ENABLE_DISTCONF:
                ReplaceStorageConfigResume(storageYaml, record.GetExpectedMainConfigVersion(),
                    record.GetExpectedStorageConfigVersion(), true);
                break;

            case EControllerOp::DISABLE_DISTCONF:
            case EControllerOp::OTHER: {
                const ui64 expectedMainYamlVersion = Self->MainConfigYamlVersion
                    ? *Self->MainConfigYamlVersion + 1
                    : 0;
                ReplaceStorageConfigResume(Self->StorageConfigYaml, expectedMainYamlVersion,
                    Self->StorageConfig->GetExpectedStorageYamlVersion(), false);
                ProposedStorageConfig.ClearConfigComposite();
                ProposedStorageConfig.ClearCompressedStorageYaml();
                ProposedStorageConfig.ClearExpectedStorageYamlVersion();
                break;
            }

            case EControllerOp::UNSET:
                Y_DEBUG_ABORT();
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr ev) {
        const auto& record = ev->Get()->Record;
        STLOG(PRI_DEBUG, BS_NODE, NWDC77, "received TEvControllerValidateConfigResponse", (SelfId, SelfId()),
            (InternalError, ev->Get()->InternalError), (Status, record.GetStatus()));

        if (ev->Get()->InternalError) {
            return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to validate config through console: "
                << *ev->Get()->InternalError);
        }

        switch (record.GetStatus()) {
            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::IdPipeServerMismatch:
                Self->DisconnectFromConsole();
                Self->ConnectToConsole();
                return FinishWithError(TResult::ERROR, TStringBuilder() << "console connection race detected: " << record.GetErrorReason());

            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid:
                return FinishWithError(TResult::ERROR, TStringBuilder() << "console config validation failed: "
                    << record.GetErrorReason());

            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid:
                if (const auto& error = UpdateConfigComposite(ProposedStorageConfig, *NewYaml, record.GetYAML())) {
                    return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to update config yaml: " << *error);
                }
                return StartProposition(&ProposedStorageConfig);
        }
    }

    void TInvokeRequestHandlerActor::BootstrapCluster(const TString& selfAssemblyUUID) {
        if (!RunCommonChecks()) {
            return;
        } else if (Self->StorageConfig->GetGeneration()) {
            if (Self->StorageConfig->GetSelfAssemblyUUID() == selfAssemblyUUID) { // repeated command, it's ok
                return Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
            } else {
                return FinishWithError(TResult::ERROR, "bootstrap on already bootstrapped cluster");
            }
        } else if (!selfAssemblyUUID) {
            return FinishWithError(TResult::ERROR, "SelfAssemblyUUID can't be empty");
        }

        const ERootState prevState = std::exchange(Self->RootState, ERootState::IN_PROGRESS);
        Y_ABORT_UNLESS(prevState == ERootState::RELAX);

        // issue scatter task to collect configs and then bootstrap cluster with specified cluster UUID
        auto done = [this, selfAssemblyUUID = TString(selfAssemblyUUID)](TEvGather *res) -> std::optional<TString> {
            Y_ABORT_UNLESS(res->HasCollectConfigs());
            Y_ABORT_UNLESS(Self->StorageConfig); // it can't just disappear
            if (Self->CurrentProposedStorageConfig) {
                FinishWithError(TResult::RACE, "config proposition request in flight");
                return std::nullopt;
            } else if (Self->StorageConfig->GetGeneration()) {
                FinishWithError(TResult::RACE, "storage config generation regenerated while collecting configs");
                return std::nullopt;
            }
            auto r = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), selfAssemblyUUID);
            return std::visit<std::optional<TString>>(TOverloaded{
                [&](std::monostate&) {
                    const ERootState prevState = std::exchange(Self->RootState, ERootState::RELAX);
                    Y_ABORT_UNLESS(prevState == ERootState::IN_PROGRESS);
                    Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
                    return std::nullopt;
                },
                [&](TString& error) {
                    const ERootState prevState = std::exchange(Self->RootState, ERootState::RELAX);
                    Y_ABORT_UNLESS(prevState == ERootState::IN_PROGRESS);
                    return error;
                },
                [&](NKikimrBlobStorage::TStorageConfig& proposedConfig) {
                    StartProposition(&proposedConfig, false);
                    return std::nullopt;
                }
            }, r.Outcome);
        };

        TEvScatter task;
        task.MutableCollectConfigs();
        IssueScatterTask(std::move(task), std::move(done));
    }

} // NKikimr::NStorage
