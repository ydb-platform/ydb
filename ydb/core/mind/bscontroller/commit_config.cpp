#include "impl.h"
#include "console_interaction.h"
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

        ui64 GenerationOnStart = 0;
        TString FingerprintOnStart;

    public:
        TTxCommitConfig(TBlobStorageController *controller, std::optional<TYamlConfig>&& yamlConfig,
                std::optional<std::optional<TString>>&& storageYamlConfig,
                std::optional<NKikimrBlobStorage::TStorageConfig>&& storageConfig)
            : TTransactionBase(controller)
            , YamlConfig(std::move(yamlConfig))
            , StorageYamlConfig(std::move(storageYamlConfig))
            , StorageConfig(std::move(storageConfig))
        {}

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_COMMIT_CONFIG; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            auto& conf = Self->StorageConfig;
            GenerationOnStart = conf.GetGeneration();
            FingerprintOnStart = conf.GetFingerprint();
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
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            auto& conf = Self->StorageConfig;
            if (conf.GetGeneration() != GenerationOnStart || conf.GetFingerprint() != FingerprintOnStart) {
                LOG_ALERT_S(ctx, NKikimrServices::BS_CONTROLLER, "Storage config changed");
                Y_DEBUG_ABORT("Storage config changed");
            }
            if (StorageConfig) {
                Self->StorageConfig = std::move(*StorageConfig);
                Self->ApplyStorageConfig(true);
            }
            if (YamlConfig) {
                Self->YamlConfig = std::move(YamlConfig);
                const auto& configVersion = GetVersion(*Self->YamlConfig);
                const auto& compressedConfig = CompressSingleConfig(*Self->YamlConfig);
                for (auto& node: Self->Nodes) {
                    if (node.second.ConnectedServerId) {
                        auto configPersistEv = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
                        auto* yamlConfig = configPersistEv->Record.MutableYamlConfig();
                        yamlConfig->SetYAML(compressedConfig);
                        yamlConfig->SetConfigVersion(configVersion);
                        Self->SendToWarden(node.first, std::move(configPersistEv), 0);
                    }
                }
            }
            if (StorageYamlConfig) {
                Self->StorageYamlConfig = std::move(*StorageYamlConfig);
            }
            Self->ConsoleInteraction->OnConfigCommit();
        }
    };

    ITransaction* TBlobStorageController::CreateTxCommitConfig(std::optional<TYamlConfig>&& yamlConfig,
            std::optional<std::optional<TString>>&& storageYamlConfig,
            std::optional<NKikimrBlobStorage::TStorageConfig>&& storageConfig) {
        return new TTxCommitConfig(this, std::move(yamlConfig), std::move(storageYamlConfig), std::move(storageConfig));
    }

} // namespace NKikimr::NBsController
