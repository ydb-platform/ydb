#include "impl.h"
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
        bool Success = true;
        std::unique_ptr<TEvBlobStorage::TEvControllerCommitConfigResponse> Response;
        std::optional<TString> ErrorReason;

    public:

        TTxCommitConfig(TBlobStorageController *controller, const TString& config, ui32 configVersion)
            : TTransactionBase(controller)
            , Config(config)
            , ConfigVersion(configVersion)
        {}

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_COMMIT_CONFIG; }
    
        bool ParseConfig(TString& config, ui32& configVersion) {
            NKikimrConfig::TAppConfig appConfig;
            try {
                appConfig = NKikimr::NYaml::Parse(config);
            } catch (const std::exception& ex) {
                Response->Record.SetStatus(NKikimrBlobStorage::TEvControllerCommitConfigResponse::ParseError);
                return false;
            }
            const bool success = NKikimr::NStorage::DeriveStorageConfig(appConfig, &StorageConfig, nullptr);
            return success;
        }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            if (!ParseConfig(Config, ConfigVersion)) {
                Success = false;
                Response->Record.SetStatus(NKikimrBlobStorage::TEvControllerCommitConfigResponse::ParseError);
                return true;
            }
            if (!(ErrorReason = NKikimr::NStorage::ValidateConfig(StorageConfig))) {
                Success = false;
                Response->Record.SetStatus(NKikimrBlobStorage::TEvControllerCommitConfigResponse::ValidationError);
                return true;
            }
            Self->StorageConfig = StorageConfig;
            db.Table<Schema::State>().Key(true).Update(
                NIceDb::TUpdate<Schema::State::YamlConfig>(Config),
                NIceDb::TUpdate<Schema::State::ConfigVersion>(ConfigVersion),
                NIceDb::TUpdate<Schema::State::IsOngoingCommit>(true));
            Self->ApplyStorageConfig();
            Self->YamlConfig = Config;
            Self->ConfigVersion = ConfigVersion;
            Self->IsOngoingCommit = true;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            if (Success) {
                Response->Record.SetStatus(NKikimrBlobStorage::TEvControllerCommitConfigResponse::Success);
            } else {
                Response->Record.SetErrorReason(*ErrorReason);
            }
            ctx.Send(Self->SelfId(), Response.release());
        }
    };

    void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerCommitConfigRequest::TPtr &ev) {
        auto& record = ev->Get()->Record;
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXCOMCONF01, "Execute TEvControllerCommitConfigRequest", (Request, record));
        Execute(new TTxCommitConfig(this, record.GetYAML(), record.GetConfigVersion()));
    }

} // namespace NKikimr::NBsController
