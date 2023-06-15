#include "impl.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxUpdateLastSeenReady : public TTransactionBase<TBlobStorageController> {
    std::vector<TVDiskAvailabilityTiming> TimingQ;

public:
    TTxUpdateLastSeenReady(std::vector<TVDiskAvailabilityTiming> timingQ, TBlobStorageController *controller)
        : TBase(controller)
        , TimingQ(std::move(timingQ))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_LAST_SEEN_READY; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& item : TimingQ) {
            auto row = db.Table<Schema::VSlot>().Key(item.VSlotId.GetKey());

#define UPDATE_CELL(CELL) \
            if (item.CELL != Schema::VSlot::CELL::Default) { \
                row.Update<Schema::VSlot::CELL>(item.CELL); \
            } else { \
                row.UpdateToNull<Schema::VSlot::CELL>(); \
            }

            UPDATE_CELL(LastSeenReady);
            UPDATE_CELL(LastGotReplicating);
            UPDATE_CELL(ReplicationTime);
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

ITransaction* TBlobStorageController::CreateTxUpdateLastSeenReady(std::vector<TVDiskAvailabilityTiming> timingQ) {
    return new TTxUpdateLastSeenReady(std::move(timingQ), this);
}

}
}
