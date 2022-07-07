#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvApplyConfig::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT06, "TEvApplyConfig", (TabletId, TabletID()), (Msg, ev->Get()->Record));

        class TTxApplyConfig : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::unique_ptr<IEventHandle> Response;
            TString ConfigProtobuf;
            bool WasConfigured = false;

        public:
            TTxApplyConfig(TBlobDepot *self, TEvBlobDepot::TEvApplyConfig& ev, std::unique_ptr<IEventHandle> response,
                    TActorId interconnectSession)
                : TTransactionBase(self)
                , Response(std::move(response))
            {
                if (interconnectSession) {
                    Response->Rewrite(TEvInterconnect::EvForward, interconnectSession);
                }
                const bool success = ev.Record.GetConfig().SerializeToString(&ConfigProtobuf);
                Y_VERIFY(success);
            }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);

                auto table = db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Select();
                if (!table.IsReady()) {
                    return false;
                } else if (table.IsValid()) {
                    WasConfigured = table.HaveValue<Schema::Config::ConfigProtobuf>();
                } else {
                    WasConfigured = false;
                }

                db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                    NIceDb::TUpdate<Schema::Config::ConfigProtobuf>(ConfigProtobuf)
                );
                return true;
            }

            void Complete(const TActorContext&) override {
                const bool success = Self->Config.ParseFromString(ConfigProtobuf);
                Y_VERIFY(success);
                if (!WasConfigured) {
                    Self->InitChannelKinds();
                }
                TActivationContext::Send(Response.release());
            }
        };

        auto responseEvent = std::make_unique<TEvBlobDepot::TEvApplyConfigResult>(TabletID(), ev->Get()->Record.GetTxId());
        auto response = std::make_unique<IEventHandle>(ev->Sender, SelfId(), responseEvent.release(), 0, ev->Cookie);
        Execute(std::make_unique<TTxApplyConfig>(this, *ev->Get(), std::move(response), ev->InterconnectSession));
    }

} // NKikimr::NBlobDepot
