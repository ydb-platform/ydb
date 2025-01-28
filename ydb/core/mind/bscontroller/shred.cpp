#include "impl.h"

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxUpdateShred : public TTransactionBase<TBlobStorageController> {
        const NKikimrBlobStorage::TEvControllerShredRequest Request;
        const TActorId Sender;
        const ui64 Cookie;
        const TActorId InterconnectSession;

    public:
        TTxUpdateShred(TBlobStorageController *controller, TEvBlobStorage::TEvControllerShredRequest::TPtr ev)
            : TTransactionBase(controller)
            , Request(std::move(ev->Get()->Record))
            , Sender(ev->Sender)
            , Cookie(ev->Cookie)
            , InterconnectSession(ev->InterconnectSession)
        {}

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_SHRED; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            auto& current = Self->ShredState;
            if (Request.HasGeneration() && (!current.HasGeneration() || current.GetGeneration() < Request.GetGeneration())) {
                // reset shred state to initial one with newly provided generation
                current.SetGeneration(Request.GetGeneration());
                current.SetCompleted(false);
                current.SetProgress10k(0);

                // serialize it to string and update database
                TString buffer;
                const bool success = current.SerializeToString(&buffer);
                Y_ABORT_UNLESS(success);
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::State>().Key(true).Update<Schema::State::ShredState>(buffer);
            }

            return true;
        }

        void Complete(const TActorContext&) override {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerShredResponse>();
            auto& r = ev->Record;
            const auto& current = Self->ShredState;
            if (current.HasGeneration()) {
                r.SetCurrentGeneration(current.GetGeneration());
                r.SetCompleted(current.GetCompleted());
                r.SetProgress10k(current.GetProgress10k());
            }

            auto h = std::make_unique<IEventHandle>(Sender, Self->SelfId(), ev.release(), 0, Cookie);
            if (InterconnectSession) {
                h->Rewrite(TEvInterconnect::EvForward, InterconnectSession);
            }
            TActivationContext::Send(h.release());
        }
    };

    void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerShredRequest::TPtr ev) {
        Execute(new TTxUpdateShred(this, ev));
    }

} // NKikimr::NBsController
