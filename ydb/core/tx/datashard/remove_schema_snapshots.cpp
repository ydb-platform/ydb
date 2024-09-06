#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class TDataShard::TTxRemoveSchemaSnapshots: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxRemoveSchemaSnapshots(TDataShard* self)
        : TBase(self)
    { }

    TTxType GetTxType() const override { return TXTYPE_REMOVE_SCHEMA_SNAPSHOTS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        while (!Self->PendingSchemaSnapshotsToGc.empty()) {
            const auto key = Self->PendingSchemaSnapshotsToGc.back();
            const auto* snapshot = Self->GetSchemaSnapshotManager().FindSnapshot(key);

            if (!snapshot) {
                Self->PendingSchemaSnapshotsToGc.pop_back();
                continue;
            }

            if (Self->GetSchemaSnapshotManager().HasReference(key)) {
                Self->PendingSchemaSnapshotsToGc.pop_back();
                continue;
            }

            auto table = Self->FindUserTable(TPathId(key.OwnerId, key.PathId));
            if (!table) {
                Self->PendingSchemaSnapshotsToGc.pop_back();
                continue;
            }

            if (snapshot->Schema->GetTableSchemaVersion() >= table->GetTableSchemaVersion()) {
                Self->PendingSchemaSnapshotsToGc.pop_back();
                continue;
            }

            Self->GetSchemaSnapshotManager().RemoveShapshot(txc.DB, key);
            Self->PendingSchemaSnapshotsToGc.pop_back();
        }

        return true;
    }

    void Complete(const TActorContext&) override {
    }
};

void TDataShard::Handle(TEvPrivate::TEvRemoveSchemaSnapshots::TPtr&, const TActorContext& ctx) {
    Execute(new TTxRemoveSchemaSnapshots(this), ctx);
}

} // namespace NKikimr::NDataShard
