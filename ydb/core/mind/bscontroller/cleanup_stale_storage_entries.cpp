#include "impl.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxCleanupStaleStorageEntries : public TTransactionBase<TBlobStorageController> {

    std::vector<Schema::BoxHostV2::TKey::Type> StaleBoxHostKeys;
    std::vector<TPDiskId> StalePDiskKeys;
    std::vector<TVSlotId> StaleVSlotKeys;

public:
    explicit TTxCleanupStaleStorageEntries(TBlobStorageController *controller)
        : TBase(controller)
        , StaleBoxHostKeys(std::move(controller->StaleBoxHostKeys))
        , StalePDiskKeys(std::move(controller->StalePDiskKeys))
        , StaleVSlotKeys(std::move(controller->StaleVSlotKeys))
    {}

    TTxType GetTxType() const override {
        return NBlobStorageController::TXTYPE_CLEANUP_STALE_STORAGE_ENTRIES;
    }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        const bool selfManagementConfigEnabled = Self->SelfManagementEnabled ||
            (Self->StorageConfig && Self->StorageConfig->GetSelfManagementConfig().GetEnabled());
        if (!selfManagementConfigEnabled) {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXCS01, "TTxCleanupStaleStorageEntries skipped, self-management disabled");
            return true;
        }

        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXCS02, "TTxCleanupStaleStorageEntries Execute",
             (BoxHosts, StaleBoxHostKeys.size()), (PDisks, StalePDiskKeys.size()), (VSlots, StaleVSlotKeys.size()));

        NIceDb::TNiceDb db(txc.DB);
        for (const auto& [boxId, fqdn, icPort] : StaleBoxHostKeys) {
            db.Table<Schema::BoxHostV2>().Key(boxId, fqdn, icPort).Delete();
        }
        for (const TVSlotId& vslotId : StaleVSlotKeys) {
            db.Table<Schema::VSlot>().Key(vslotId.NodeId, vslotId.PDiskId, vslotId.VSlotId).Delete();
        }
        for (const TPDiskId& pdiskId : StalePDiskKeys) {
            db.Table<Schema::PDisk>().Key(pdiskId.NodeId, pdiskId.PDiskId).Delete();
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

ITransaction* TBlobStorageController::CreateTxCleanupStaleStorageEntries() {
    return new TTxCleanupStaleStorageEntries(this);
}

} // namespace NBsController
} // namespace NKikimr
