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
            NKikimrBlobDepot::TBlobDepotConfig Next;
            TString ConfigProtobuf;
            bool RestartRequired = false;

        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_APPLY_CONFIG; }

            TTxApplyConfig(TBlobDepot *self, TEvBlobDepot::TEvApplyConfig& ev, std::unique_ptr<IEventHandle> response,
                    TActorId interconnectSession)
                : TTransactionBase(self)
                , Response(std::move(response))
                , Next(ev.Record.GetConfig())
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

                if (!Self->Configured && !Self->Config.HasBridgeGroupGeneration() && Next.HasBridgeGroupGeneration()) {
                    // The first transaction can still be awaiting completion while its successor executes.
                    // Install the initial fence now; admission stays closed until StartOperation().
                    Self->Config = Next;
                }
                if (Self->Config.HasBridgeGroupGeneration()) {
                    const ui32 generation = Next.GetBridgeGroupGeneration();
                    if (generation < Self->Config.GetBridgeGroupGeneration() ||
                            Next.GetBackupConfigRevision() < Self->Config.GetBackupConfigRevision()) {
                        return true; // a retried controller request from an older stage
                    }
                    // Transactions executed after this update must apply the new fence, even before its completion.
                    Self->Config.SetBridgeGroupGeneration(generation);
                    Self->Config.SetBackupConfigRevision(Next.GetBackupConfigRevision());
                }
                const bool changed = !table.IsValid() || !table.HaveValue<Schema::Config::ConfigProtobuf>() ||
                    table.GetValue<Schema::Config::ConfigProtobuf>() != ConfigProtobuf;
                if (changed) {
                    RestartRequired = !Self->Config.HasBridgeGroupGeneration() || Self->Config.SerializeAsString() != ConfigProtobuf;
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
                    if (!Self->Config.HasBridgeGroupGeneration()) {
                        Self->Config = Next;
                    }
                    Self->StartOperation();
                }
                TActivationContext::Send(Response.release());
                if (wasConfigured && RestartRequired) {
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
