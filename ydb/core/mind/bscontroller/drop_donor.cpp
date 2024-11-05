#include "impl.h"
#include "config.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxDropDonor
    : public TTransactionBase<TBlobStorageController>
{
    std::vector<TVSlotId> VSlotIds;
    std::optional<TConfigState> State;

public:
    TTxDropDonor(TEvPrivate::TEvDropDonor::TPtr ev, TBlobStorageController *controller)
        : TTransactionBase(controller)
        , VSlotIds(std::move(ev->Get()->VSlotIds))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_DROP_DONOR; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());
        State->CheckConsistency();
        for (const TVSlotId& vslotId : VSlotIds) {
            if (const TVSlotInfo *vslot = State->VSlots.Find(vslotId); vslot && !vslot->IsBeingDeleted()) {
                Y_ABORT_UNLESS(vslot->Mood == TMood::Donor);
                State->DestroyVSlot(vslotId);
            }
        }
        State->CheckConsistency();
        TString error;
        if (State->Changed() && !Self->CommitConfigUpdates(*State, false, false, false, txc, &error)) {
            State->Rollback();
            State.reset();
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        if (State) {
            State->ApplyConfigUpdates();
            State.reset();
        }
    }
};

void TBlobStorageController::Handle(TEvPrivate::TEvDropDonor::TPtr ev) {
    Execute(new TTxDropDonor(ev, this));
}

} // NKikimr::NBsController
