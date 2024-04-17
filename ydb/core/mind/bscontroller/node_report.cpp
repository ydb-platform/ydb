#include "impl.h"
#include "config.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxNodeReport
    : public TTransactionBase<TBlobStorageController>
{
    TEvBlobStorage::TEvControllerNodeReport::TPtr Event;
    std::optional<TConfigState> State;

public:
    TTxNodeReport(TEvBlobStorage::TEvControllerNodeReport::TPtr &ev, TBlobStorageController *controller)
        : TBase(controller)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_NODE_REPORT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TRequestCounter counter(Self->TabletCounters, NBlobStorageController::COUNTER_NODE_REPORT_USEC);

        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXNR01, "TTxNodeReport execute");

        if (!Self->ValidateIncomingNodeWardenEvent(*Event)) {
            return true;
        }

        State.emplace(*Self, Self->HostRecords, TActivationContext::Now());
        State->CheckConsistency();

        NIceDb::TNiceDb db(txc.DB);
        const auto& record = Event->Get()->Record;

        if (!record.HasNodeId()) {
            return true;
        }
        for (const auto& report : record.GetVDiskReports()) {
            if (!report.HasVSlotId() || !report.HasVDiskId() || !report.HasPhase()) {
                continue; // ignore incorrect report
            }

            TVSlotInfo *slot = State->VSlots.FindForUpdate(report.GetVSlotId());
            if (!slot || !slot->IsSameVDisk(VDiskIDFromVDiskID(report.GetVDiskId()))) {
                continue; // ignore entry -- possibly race/obsolete reply
            }

            switch (report.GetPhase()) {
                case NKikimrBlobStorage::TEvControllerNodeReport::UNKNOWN:
                    continue; // ignore incorrect report

                case NKikimrBlobStorage::TEvControllerNodeReport::WIPED:
                    if (slot->Mood == TMood::Wipe) {
                        slot->Mood = TMood::Normal;
                    }
                    break;

                case NKikimrBlobStorage::TEvControllerNodeReport::DESTROYED:
                    if (slot->IsBeingDeleted()) {
                        const size_t num = const_cast<TPDiskInfo&>(*slot->PDisk).VSlotsOnPDisk.erase(slot->VSlotId.VSlotId);
                        Y_ABORT_UNLESS(num);
                        State->DeleteDestroyedVSlot(slot);
                    }
                    break;

                case NKikimrBlobStorage::TEvControllerNodeReport::OPERATION_ERROR:
                    switch (slot->Mood) { // TODO(alexvru): implement some retry logic?
                        case TMood::Normal:
                            break;
                        case TMood::Wipe:
                            break;
                        case TMood::Delete:
                            break;
                        case TMood::Donor:
                            break;
                    }
                    break;
            }
        }

        for (const auto& report : record.GetPDiskReports()) {
            if (!report.HasPDiskId() || !report.HasPhase()) {
                continue; // ignore incorrect report
            }

            TPDiskId pdiskId(record.GetNodeId(), report.GetPDiskId());

            TPDiskInfo *pdisk = State->PDisks.FindForUpdate(pdiskId);
            if (!pdisk) {
                continue;
            }

            switch (report.GetPhase()) {
                case NKikimrBlobStorage::TEvControllerNodeReport::PD_UNKNOWN:
                    continue;

                case NKikimrBlobStorage::TEvControllerNodeReport::PD_RESTARTED:
                    if (pdisk->Mood == TPDiskMood::Restarting) {
                        pdisk->Mood = TPDiskMood::Normal;
                    }
                    break;
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
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXNR02, "TTxNodeReport complete");
        if (State) {
            State->ApplyConfigUpdates();
            State.reset();
        }
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerNodeReport::TPtr &ev) {
    TabletCounters->Cumulative()[NBlobStorageController::COUNTER_NODE_REPORT_COUNT].Increment(1);
    TRequestCounter counter(TabletCounters, NBlobStorageController::COUNTER_NODE_REPORT_USEC);
    Execute(new TTxNodeReport(ev, this));
}

} // NKikimr::NBsController
