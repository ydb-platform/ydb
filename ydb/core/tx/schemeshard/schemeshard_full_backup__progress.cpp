#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>

// OnItemDone records per-item progress (progress percentage only; does not finalize the tracked row).
// Finalize is called on control op completion and is the sole durable source of truth for the terminal state.

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TFullBackup::TTxProgress: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    explicit TTxProgress(TSelf* self, ui64 id)
        : TBase(self)
        , Id(id)
    {}

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvFullBackupItemDone::TPtr& ev)
        : TBase(self)
        , ItemDone(ev)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_PROGRESS_FULL_BACKUP;
    }

    bool OnItemDone(TTransactionContext& txc, const TActorContext&) {
        auto& msg = *ItemDone->Get();

        auto* infoPtr = Self->FullBackups.FindPtr(msg.FullBackupId);
        if (!infoPtr) {
            return true;
        }
        auto& info = **infoPtr;
        if (info.IsFinished()) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        // Lazily register the item on first event: dst TPathId is not known until the CopyTable sub-op Propose.
        auto& item = info.Items[msg.DstPathId];
        if (!item.PathId) {
            item.PathId = msg.DstPathId;
            item.State = TFullBackupInfo::TItem::EState::Transferring;
        }

        if (item.State == TFullBackupInfo::TItem::EState::Done ||
            item.State == TFullBackupInfo::TItem::EState::Failed) {
            return true;
        }

        item.State = msg.Success
            ? TFullBackupInfo::TItem::EState::Done
            : TFullBackupInfo::TItem::EState::Failed;
        Self->PersistFullBackupItem(db, info.Id, item);

        return true;
    }

    bool Finalize(TTransactionContext& txc, const TActorContext& ctx) {
        NIceDb::TNiceDb db(txc.DB);
        Self->FinalizeFullBackupOnOpComplete(db, Id, ctx);
        return true;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        bool ok = ItemDone ? OnItemDone(txc, ctx) : Finalize(txc, ctx);
        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return ok;
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    TSideEffects SideEffects;
    ui64 Id = 0;
    TEvPrivate::TEvFullBackupItemDone::TPtr ItemDone;
};

ITransaction* TSchemeShard::CreateTxFullBackupProgress(ui64 id) {
    return new TFullBackup::TTxProgress(this, id);
}

ITransaction* TSchemeShard::CreateTxFullBackupProgress(TEvPrivate::TEvFullBackupItemDone::TPtr& ev) {
    return new TFullBackup::TTxProgress(this, ev);
}

} // namespace NKikimr::NSchemeShard
