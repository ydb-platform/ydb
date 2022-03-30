#include "columnshard_impl.h"
#include "blob_manager_db.h"

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxExport : public TTransactionBase<TColumnShard> {
public:
    TTxExport(TColumnShard* self, TEvPrivate::TEvExport::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_EXPORT; }

private:
    TEvPrivate::TEvExport::TPtr Ev;
};


bool TTxExport::Execute(TTransactionContext& txc, const TActorContext&) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG("TTxExport.Execute at tablet " << Self->TabletID());

    txc.DB.NoMoreReadsForTx();
    //NIceDb::TNiceDb db(txc.DB);

    auto& msg = *Ev->Get();
    auto status = msg.Status;

    if (status == NKikimrProto::OK) {
        TBlobManagerDb blobManagerDb(txc.DB);

        for (auto& [blobId, externId] : msg.SrcToDstBlobs) {
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
                Self->BlobManager->DeleteBlob(blobId, blobManagerDb);
                Self->IncCounter(COUNTER_BLOBS_ERASED);
                Self->IncCounter(COUNTER_BYTES_ERASED, blobId.BlobSize());
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

void TTxExport::Complete(const TActorContext&) {
    LOG_S_DEBUG("TTxExport.Complete at tablet " << Self->TabletID());
}


void TColumnShard::Handle(TEvPrivate::TEvExport::TPtr& ev, const TActorContext& ctx) {
    auto status = ev->Get()->Status;
    bool error = status == NKikimrProto::ERROR;

    if (error) {
        LOG_S_WARN("Export (fail): '" << ev->Get()->ErrorStr << "' at tablet " << TabletID());
    } else if (status == NKikimrProto::UNKNOWN) {
        ui64 exportNo = ev->Get()->ExportNo;
        auto& tierBlobs = ev->Get()->TierBlobs;
        Y_VERIFY(tierBlobs.size());

        LOG_S_DEBUG("Export (write): " << exportNo << " at tablet " << TabletID());

        for (auto& [tierName, blobIds] : tierBlobs) {
            if (!S3Actors.count(tierName)) {
                TString tier(tierName);
                LOG_S_ERROR("No S3 actor for tier '" << tier << "' (on export) at tablet " << TabletID());
                continue;
            }
            auto& s3 = S3Actors[tierName];
            if (!s3) {
                TString tier(tierName);
                LOG_S_ERROR("Not started S3 actor for tier '" << tier << "' (on export) at tablet " << TabletID());
                continue;
            }
            auto event = std::make_unique<TEvPrivate::TEvExport>(exportNo, s3, std::move(blobIds));
            ctx.Register(CreateExportActor(TabletID(), ctx.SelfID, event.release()));
        }
    } else if (status == NKikimrProto::OK) {
        LOG_S_DEBUG("Export (apply) at tablet " << TabletID());

        Execute(new TTxExport(this, ev), ctx);
    } else {
        Y_VERIFY(false);
    }
}

}
