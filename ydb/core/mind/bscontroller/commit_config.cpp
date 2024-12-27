#include "impl.h"
#include "console_interaction.h"
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxCommitConfig
        : public TTransactionBase<TBlobStorageController>
    {
        TString Config;
        ui32 ConfigVersion;
        NKikimrBlobStorage::TStorageConfig StorageConfig;
        TConsoleInteraction::TCommitConfigResult Result;
        NKikimrBlobStorage::TStorageConfig BSCStorageConfig;

    public:
        TTxCommitConfig(TBlobStorageController *controller, const TString& config, ui32 configVersion, NKikimrBlobStorage::TStorageConfig& storageConfig)
            : TTransactionBase(controller)
            , Config(config)
            , ConfigVersion(configVersion)
            , StorageConfig(storageConfig)
        {}

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_COMMIT_CONFIG; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            BSCStorageConfig = Self->StorageConfig;
            db.Table<Schema::State>().Key(true).Update(
                NIceDb::TUpdate<Schema::State::YamlConfig>(Config),
                NIceDb::TUpdate<Schema::State::ConfigVersion>(ConfigVersion));
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            if (BSCStorageConfig.GetGeneration() != Self->StorageConfig.GetGeneration()) {
                LOG_ALERT_S(ctx, NKikimrServices::BS_CONTROLLER, "Storage config changed");
                Y_VERIFY_DEBUG_S(false, "Storage config changed");
            }
            Self->StorageConfig = StorageConfig;
            Self->ApplyStorageConfig();
            Self->YamlConfig = Config;
            Self->ConfigVersion = ConfigVersion;
            Result.Status = TConsoleInteraction::TCommitConfigResult::EStatus::Success;
            Self->ConsoleInteraction->OnConfigCommit(Result);
        }
    };

    ITransaction* TBlobStorageController::CreateTxCommitConfig(TString& yamlConfig, ui64 configVersion, NKikimrBlobStorage::TStorageConfig& storageConfig) {
        return new TTxCommitConfig(this, yamlConfig, configVersion, storageConfig);
    }

} // namespace NKikimr::NBsController
