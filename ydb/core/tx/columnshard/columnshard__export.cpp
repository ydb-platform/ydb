#include "columnshard_impl.h"
#include "blob_manager_db.h"
#include "columnshard_schema.h"

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxExportFinish: public TTransactionBase<TColumnShard> {
public:
    TTxExportFinish(TColumnShard* self, TEvPrivate::TEvExport::TPtr& ev)
        : TBase(self)
        , Ev(ev) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_EXPORT; }

private:
    TEvPrivate::TEvExport::TPtr Ev;
    std::vector<NOlap::TEvictedBlob> BlobsToForget;
};


bool TTxExportFinish::Execute(TTransactionContext& txc, const TActorContext&) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG("TTxExportFinish.Execute at tablet " << Self->TabletID());

    txc.DB.NoMoreReadsForTx();
    //NIceDb::TNiceDb db(txc.DB);

    auto& msg = *Ev->Get();
    auto status = msg.Status;

    if (status == NKikimrProto::OK) {
        TBlobManagerDb blobManagerDb(txc.DB);

        for (auto& [blob, externId] : msg.SrcToDstBlobs) {
            auto& blobId = blob;
            Y_VERIFY(blobId.IsDsBlob());
            Y_VERIFY(externId.IsS3Blob());
            bool dropped = false;

#if 0 // TODO: SELF_CACHED logic
            NOlap::TEvictedBlob evict{
                .State = EEvictState::SELF_CACHED,
                .Blob = blobId,
                .ExternBlob = externId
            };
            Self->BlobManager->UpdateOneToOne(std::move(evict), blobManagerDb, dropped);
#else
            NOlap::TEvictedBlob evict{
                .State = EEvictState::EXTERN,
                .Blob = blobId,
                .ExternBlob = externId
            };
            bool present = Self->BlobManager->UpdateOneToOne(std::move(evict), blobManagerDb, dropped);

            // Delayed erase of evicted blob. Blob could be already deleted.
            if (present && !dropped) {
                LOG_S_DEBUG("Delete exported blob '" << blobId.ToStringNew() << "' at tablet " << Self->TabletID());
                Self->BlobManager->DeleteBlob(blobId, blobManagerDb);
                Self->IncCounter(COUNTER_BLOBS_ERASED);
                Self->IncCounter(COUNTER_BYTES_ERASED, blobId.BlobSize());
            } else if (present) {
                LOG_S_DEBUG("Stale exported blob '" << blobId.ToStringNew() << "' at tablet " << Self->TabletID());

                TEvictMetadata meta;
                evict = Self->BlobManager->GetDropped(blobId, meta);
                Y_VERIFY(evict.State == EEvictState::EXTERN);

                if (Self->DelayedForgetBlobs.count(blobId)) {
                    Self->DelayedForgetBlobs.erase(blobId);
                    BlobsToForget.emplace_back(std::move(evict));
                } else {
                    LOG_S_ERROR("No delayed forget for stale exported blob '"
                        << blobId.ToStringNew() << "' at tablet " << Self->TabletID());
                }
            } else {
                LOG_S_ERROR("Exported but unknown blob '" << blobId.ToStringNew() << "' at tablet " << Self->TabletID());
            }

            // TODO: delete not present in S3 for sure (avoid race between export and forget)
#endif
        }

        Self->IncCounter(COUNTER_EXPORT_SUCCESS);
    } else {
        Self->IncCounter(COUNTER_EXPORT_FAIL);
    }

    return true;
}

void TTxExportFinish::Complete(const TActorContext& ctx) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG("TTxExportFinish.Complete at tablet " << Self->TabletID());

    auto& msg = *Ev->Get();
    Y_VERIFY(!msg.TierName.empty());
    Self->ActiveEviction = false;
    if (!BlobsToForget.empty()) {
        Self->ForgetBlobs(ctx, msg.TierName, std::move(BlobsToForget));
    }
}


void TColumnShard::Handle(TEvPrivate::TEvExport::TPtr& ev, const TActorContext& ctx) {
    auto status = ev->Get()->Status;

    Y_VERIFY(!ActiveTtl, "TTL already in progress at tablet %lu", TabletID());
    Y_VERIFY(!ActiveEviction || status != NKikimrProto::UNKNOWN, "Eviction in progress at tablet %lu", TabletID());
    ui64 exportNo = ev->Get()->ExportNo;
    auto& tierName = ev->Get()->TierName;

    if (status == NKikimrProto::ERROR) {
        LOG_S_WARN("Export (fail): " << exportNo << " tier '" << tierName << "' error: "
            << ev->Get()->SerializeErrorsToString() << "' at tablet " << TabletID());
        ActiveEviction = false;
    } else if (status == NKikimrProto::UNKNOWN) {
        LOG_S_DEBUG("Export (write): " << exportNo << " tier '" << tierName << "' at tablet " << TabletID());
        auto& tierBlobs = ev->Get()->Blobs;
        Y_VERIFY(tierBlobs.size());
        ExportBlobs(ctx, exportNo, tierName, std::move(tierBlobs));
    } else if (status == NKikimrProto::OK) {
        LOG_S_DEBUG("Export (apply): " << exportNo << " tier '" << tierName << "' at tablet " << TabletID());
        Execute(new TTxExportFinish(this, ev), ctx);
    } else {
        Y_VERIFY(false);
    }
    ActiveEviction = true;
}

}
