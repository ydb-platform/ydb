#include "defs.h"
#include "columnshard_impl.h"
#include "blob_manager.h"

#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NColumnShard {

// Run GC related logic of the BlobManager
class TTxRunGC : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
    THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>> Requests;
    bool Cleanup = false;

public:
    TTxRunGC(TColumnShard* self)
        : TBase(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_S_TRACE("TTxRunGC.Execute at tablet " << Self->TabletID());
        Y_UNUSED(ctx);

        // Cleanup delayed blobs before next GC
        TBlobManagerDb blobManagerDb(txc.DB);
        if (Self->BlobManager->CleanupFlaggedBlobs(blobManagerDb, TLimits::MAX_BLOBS_TO_DELETE)) {
            Cleanup = true;
            return true;
        }

        Requests = Self->BlobManager->PreparePerGroupGCRequests();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_S_TRACE("TTxRunGC.Complete at tablet " << Self->TabletID());

        /// @warning it's a loop Complete -> Execute. We must exit from it fo sure.
        if (Cleanup) {
            Self->ScheduleNextGC(ctx, true);
        }

        for (auto& r : Requests) {
            ui32 groupId = r.first;
            auto ev = std::move(r.second);
            LOG_S_DEBUG("BlobManager at tablet " << Self->TabletID()
                << " Sending GC to group " << groupId << ": " << ev->Print(true));

            SendToBSProxy(ctx, groupId, ev.release());
        }
    }
};

ITransaction* TColumnShard::CreateTxRunGc() {
    return new TTxRunGC(this);
}


// Update the BlobManager with the GC result
class TTxProcessGCResult : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
    TEvBlobStorage::TEvCollectGarbageResult::TPtr Ev;
public:
    TTxProcessGCResult(TColumnShard* self, TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (Ev->Get()->Status != NKikimrProto::OK) {
            LOG_S_WARN("BlobManager at tablet " << Self->TabletID()
                << " GC Failed: " << Ev->Get()->Print(true));
            Self->BecomeBroken(ctx);
            return true;
        }

        LOG_S_DEBUG("BlobManager at tablet " << Self->TabletID()
            << " GC Result: " << Ev->Get()->Print(true));

        // Update Keep/DontKeep lists and last GC barrier
        TBlobManagerDb blobManagerDb(txc.DB);
        Self->BlobManager->OnGCResult(Ev, blobManagerDb);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->ScheduleNextGC(ctx);
    }
};

void TColumnShard::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxProcessGCResult(this, ev), ctx);
}

}
