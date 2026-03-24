#include "impl.h"
#include "console_interaction.h"
#include "bsc_audit.h"
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxCommitConfig
        : public TTransactionBase<TBlobStorageController>
    {
        std::optional<TYamlConfig> YamlConfig;
        std::optional<std::optional<TString>> StorageYamlConfig;
        std::optional<NKikimrBlobStorage::TStorageConfig> StorageConfig;
        std::optional<ui64> ExpectedStorageYamlConfigVersion;
        std::unique_ptr<IEventHandle> Handle;
        std::optional<bool> SwitchEnableConfigV2;
        std::optional<TAuditLogInfo> AuditLogInfo;

        ui64 GenerationOnStart = 0;
        TString FingerprintOnStart;

    public:
        TTxCommitConfig(TBlobStorageController *controller, std::optional<TYamlConfig>&& yamlConfig,
                std::optional<std::optional<TString>>&& storageYamlConfig,
                std::optional<NKikimrBlobStorage::TStorageConfig>&& storageConfig,
                std::optional<ui64> expectedStorageYamlConfigVersion, std::unique_ptr<IEventHandle> handle,
                std::optional<bool> switchEnableConfigV2, std::optional<TAuditLogInfo>&& auditLogInfo)
            : TTransactionBase(controller)
            , YamlConfig(std::move(yamlConfig))
            , StorageYamlConfig(std::move(storageYamlConfig))
            , StorageConfig(std::move(storageConfig))
            , ExpectedStorageYamlConfigVersion(expectedStorageYamlConfigVersion)
            , Handle(std::move(handle))
            , SwitchEnableConfigV2(switchEnableConfigV2)
            , AuditLogInfo(std::move(auditLogInfo))
        {}

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_COMMIT_CONFIG; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXCFG03, "executing TTxCommitConfig");
            NIceDb::TNiceDb db(txc.DB);
            auto& conf = Self->StorageConfig;
            GenerationOnStart = conf->GetGeneration();
            FingerprintOnStart = conf->GetFingerprint();
            auto row = db.Table<Schema::State>().Key(true);
            if (YamlConfig) {
                row.Update<Schema::State::YamlConfig>(CompressYamlConfig(*YamlConfig));
            }
            if (StorageYamlConfig) {
                if (*StorageYamlConfig) {
                    row.Update<Schema::State::StorageYamlConfig>(CompressStorageYamlConfig(**StorageYamlConfig));
                } else {
                    row.UpdateToNull<Schema::State::StorageYamlConfig>();
                }
            }
            if (ExpectedStorageYamlConfigVersion) {
                row.Update<Schema::State::ExpectedStorageYamlConfigVersion>(*ExpectedStorageYamlConfigVersion);
            }
            if (SwitchEnableConfigV2) {
                row.Update<Schema::State::EnableConfigV2>(*SwitchEnableConfigV2);
            }
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXCFG04, "completing TTxCommitConfig");
            auto& conf = Self->StorageConfig;
            if (conf->GetGeneration() != GenerationOnStart || conf->GetFingerprint() != FingerprintOnStart) {
                LOG_ALERT_S(ctx, NKikimrServices::BS_CONTROLLER, "Storage config changed");
                Y_DEBUG_ABORT("Storage config changed");
            }

            if (AuditLogInfo) {
                TStringBuilder oldConfig;
                if (Self->YamlConfig) {
                    oldConfig << GetSingleConfigYaml(*Self->YamlConfig);
                }
                if (Self->StorageYamlConfig) {
                    oldConfig << *Self->StorageYamlConfig;
                }

                TStringBuilder newConfig;
                if (YamlConfig) {
                    newConfig << GetSingleConfigYaml(*YamlConfig);
                }
                if (StorageYamlConfig && *StorageYamlConfig) {
                    newConfig << **StorageYamlConfig;
                }

                AuditLogCommitConfigTransaction(
                    /* peer = */ AuditLogInfo->PeerName,
                    /* userSID = */ AuditLogInfo->UserToken.GetUserSID(),
                    /* sanitizedToken = */ AuditLogInfo->UserToken.GetSanitizedToken(),
                    /* oldConfig = */ oldConfig,
                    /* newConfig = */ newConfig,
                    /* reason = */ {},
                    /* success = */ true);
            }

            if (StorageConfig) {
                Self->StorageConfig = std::make_shared<NKikimrBlobStorage::TStorageConfig>(*StorageConfig);
                Self->ApplyStorageConfig(true);
            }

            std::optional<NKikimrBlobStorage::TYamlConfig> update;

            if (YamlConfig) {
                Self->YamlConfig = std::move(YamlConfig);
                Self->YamlConfigHash = GetSingleConfigHash(*Self->YamlConfig);

                if (!update) {
                    update.emplace();
                }
                update->SetCompressedMainConfig(CompressSingleConfig(*Self->YamlConfig));
                update->SetMainConfigVersion(GetVersion(*Self->YamlConfig));
            }
            if (StorageYamlConfig) {
                const bool hadStorageConfigBefore = Self->StorageYamlConfig.has_value();

                Self->StorageYamlConfig = std::move(*StorageYamlConfig);
                Self->StorageYamlConfigVersion = 0;
                Self->StorageYamlConfigHash = 0;

                if (Self->StorageYamlConfig) {
                    Self->StorageYamlConfigVersion = NYamlConfig::GetStorageMetadata(*Self->StorageYamlConfig).Version.value_or(0);
                    Self->StorageYamlConfigHash = NYaml::GetConfigHash(*Self->StorageYamlConfig);

                    if (!update) {
                        update.emplace();
                    }
                    update->SetCompressedStorageConfig(CompressStorageYamlConfig(*Self->StorageYamlConfig));
                } else if (hadStorageConfigBefore && !update) {
                    update.emplace(); // issue an update without storage yaml version meaning we are in single-config mode
                }
            }
            if (ExpectedStorageYamlConfigVersion) {
                Self->ExpectedStorageYamlConfigVersion = *ExpectedStorageYamlConfigVersion;
            }
            if (update && Self->StorageYamlConfig) {
                update->SetStorageConfigVersion(NYamlConfig::GetStorageMetadata(*Self->StorageYamlConfig).Version.value_or(0));
            }
            if (SwitchEnableConfigV2) {
                STLOG(PRI_INFO, BS_CONTROLLER, BSCTXCFG01, "updating EnableConfigV2", (Value, *SwitchEnableConfigV2));
                Self->EnableConfigV2 = *SwitchEnableConfigV2;
            }

            if (Handle) {
                TActivationContext::Send(Handle.release());
            } else {
                Self->ConsoleInteraction->OnConfigCommit();
            }

            if (update) {
                STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXCFG02, "send persist new config command to connected nodes");
                for (auto& node: Self->Nodes) {
                    if (node.second.ConnectedServerId) {
                        auto configPersistEv = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
                        configPersistEv->Record.MutableYamlConfig()->CopyFrom(*update);
                        Self->SendToWarden(node.first, std::move(configPersistEv), 0);
                    }
                }
            }
        }
    };

    ITransaction* TBlobStorageController::CreateTxCommitConfig(std::optional<TYamlConfig>&& yamlConfig,
            std::optional<std::optional<TString>>&& storageYamlConfig,
            std::optional<NKikimrBlobStorage::TStorageConfig>&& storageConfig,
            std::optional<ui64> expectedStorageYamlConfigVersion, std::unique_ptr<IEventHandle> handle,
            std::optional<bool> switchEnableConfigV2, std::optional<TAuditLogInfo>&& auditLogInfo) {
        return new TTxCommitConfig(this, std::move(yamlConfig), std::move(storageYamlConfig), std::move(storageConfig),
            expectedStorageYamlConfigVersion, std::move(handle), switchEnableConfigV2, std::move(auditLogInfo));
    }

} // namespace NKikimr::NBsController
