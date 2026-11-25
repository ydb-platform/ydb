#include "blob_depot_tablet.h"
#include "schema.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvApplyConfig::TPtr ev) {
        YDB_LOG_DEBUG("TEvApplyConfig",
            {"marker", "BDT15"},
            {"id", GetLogId()},
            {"msg", ev->Get()->Record});

        class TTxApplyConfig : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::unique_ptr<IEventHandle> Response;
            TString ConfigProtobuf;
            bool Changed = false;

        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_APPLY_CONFIG; }

            TTxApplyConfig(TBlobDepot *self, TEvBlobDepot::TEvApplyConfig& ev, std::unique_ptr<IEventHandle> response,
                    TActorId interconnectSession)
                : TTransactionBase(self)
                , Response(std::move(response))
            {
                if (interconnectSession) {
                    Response->Rewrite(TEvInterconnect::EvForward, interconnectSession);
                }
                const bool success = ev.Record.GetConfig().SerializeToString(&ConfigProtobuf);
                Y_ABORT_UNLESS(success);
            }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                YDB_LOG_DEBUG("TTxApplyConfig::Execute",
                    {"marker", "BDT16"},
                    {"id", Self->GetLogId()});

                NIceDb::TNiceDb db(txc.DB);

                auto table = db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Select();
                if (!table.IsReady()) {
                    return false;
                }

                Changed = !table.IsValid() || !table.HaveValue<Schema::Config::ConfigProtobuf>() ||
                    table.GetValue<Schema::Config::ConfigProtobuf>() != ConfigProtobuf;
                if (Changed) {
                    db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                        NIceDb::TUpdate<Schema::Config::ConfigProtobuf>(ConfigProtobuf));
                }

                return true;
            }

            void Complete(const TActorContext&) override {
                YDB_LOG_DEBUG("TTxApplyConfig::Complete",
                    {"marker", "BDT17"},
                    {"id", Self->GetLogId()});

                const bool wasConfigured = std::exchange(Self->Configured, true);
                if (!wasConfigured) {
                    const bool success = Self->Config.ParseFromString(ConfigProtobuf);
                    Y_ABORT_UNLESS(success);
                    Self->StartOperation();
                }
                TActivationContext::Send(Response.release());
                if (wasConfigured && Changed) {
                    Self->RestartingForConfigChange = true;
                    Self->Send(Self->Tablet(), new TEvents::TEvPoison);
                }
            }
        };

        auto responseEvent = std::make_unique<TEvBlobDepot::TEvApplyConfigResult>(TabletID(), ev->Get()->Record.GetTxId());
        auto response = std::make_unique<IEventHandle>(ev->Sender, SelfId(), responseEvent.release(), 0, ev->Cookie);
        Execute(std::make_unique<TTxApplyConfig>(this, *ev->Get(), std::move(response), ev->InterconnectSession));
    }

} // NKikimr::NBlobDepot
