#include "impl.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxUpdateLastSeenReady : public TTransactionBase<TBlobStorageController> {
    std::vector<std::pair<TVSlotId, TInstant>> LastSeenReadyQ;

public:
    TTxUpdateLastSeenReady(std::vector<std::pair<TVSlotId, TInstant>> lastSeenReadyQ, TBlobStorageController *controller)
        : TBase(controller)
        , LastSeenReadyQ(std::move(lastSeenReadyQ))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_LAST_SEEN_READY; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& [vslotId, lastSeenReady] : LastSeenReadyQ) {
            db.Table<Schema::VSlot>().Key(vslotId.GetKey()).Update<Schema::VSlot::LastSeenReady>(lastSeenReady);
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

ITransaction* TBlobStorageController::CreateTxUpdateLastSeenReady(std::vector<std::pair<TVSlotId, TInstant>> lastSeenReadyQ) {
    return new TTxUpdateLastSeenReady(std::move(lastSeenReadyQ), this);
}

}
}
