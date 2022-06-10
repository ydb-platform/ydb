#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvApplyConfig::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "TEvApplyConfig", (TabletId, TabletID()), (Msg, ev->Get()->Record));

        class TTxApplyConfig : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::unique_ptr<IEventHandle> Response;
            const TActorId InterconnectSession;
            TString ConfigProtobuf;

        public:
            TTxApplyConfig(TBlobDepot *self, TEvBlobDepot::TEvApplyConfig& ev, std::unique_ptr<IEventHandle> response,
                    TActorId interconnectSession)
                : TTransactionBase(self)
                , Response(std::move(response))
                , InterconnectSession(interconnectSession)
            {
                const bool success = ev.Record.SerializeToString(&ConfigProtobuf);
                Y_VERIFY(success);
            }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                    NIceDb::TUpdate<Schema::Config::ConfigProtobuf>(ConfigProtobuf)
                );
                return true;
            }

            void Complete(const TActorContext&) override {
                const bool success = Self->Config.ParseFromString(ConfigProtobuf);
                Y_VERIFY(success);
                if (InterconnectSession) {
                    Response->Rewrite(TEvInterconnect::EvForward, InterconnectSession);
                }
                TActivationContext::Send(Response.release());
            }
        };

        auto responseEvent = std::make_unique<TEvBlobDepot::TEvApplyConfigResult>(TabletID(), ev->Get()->Record.GetTxId());
        auto response = std::make_unique<IEventHandle>(ev->Sender, SelfId(), responseEvent.release(), 0, ev->Cookie);
        Execute(std::make_unique<TTxApplyConfig>(this, *ev->Get(), std::move(response), ev->InterconnectSession));
    }

} // NKikimr::NBlobDepot
