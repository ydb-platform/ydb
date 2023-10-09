#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvApplyConfig::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT15, "TEvApplyConfig", (Id, GetLogId()), (Msg, ev->Get()->Record));

        class TTxApplyConfig : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::unique_ptr<IEventHandle> Response;
            TString ConfigProtobuf;

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
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT16, "TTxApplyConfig::Execute", (Id, Self->GetLogId()));

                NIceDb::TNiceDb db(txc.DB);

                auto table = db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Select();
                if (!table.IsReady()) {
                    return false;
                }

                db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                    NIceDb::TUpdate<Schema::Config::ConfigProtobuf>(ConfigProtobuf)
                );

                const bool success = Self->Config.ParseFromString(ConfigProtobuf);
                Y_ABORT_UNLESS(success);

                return true;
            }

            void Complete(const TActorContext&) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT17, "TTxApplyConfig::Complete", (Id, Self->GetLogId()));

                if (!std::exchange(Self->Configured, true)) {
                    Self->StartOperation();
                }

                TActivationContext::Send(Response.release());
            }
        };

        auto responseEvent = std::make_unique<TEvBlobDepot::TEvApplyConfigResult>(TabletID(), ev->Get()->Record.GetTxId());
        auto response = std::make_unique<IEventHandle>(ev->Sender, SelfId(), responseEvent.release(), 0, ev->Cookie);
        Execute(std::make_unique<TTxApplyConfig>(this, *ev->Get(), std::move(response), ev->InterconnectSession));
    }

} // NKikimr::NBlobDepot
