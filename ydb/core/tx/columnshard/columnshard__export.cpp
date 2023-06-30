#include "columnshard_impl.h"
#include "blob_manager_db.h"
#include "columnshard_schema.h"

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxExportFinish: public TTransactionBase<TColumnShard> {
public:
    TTxExportFinish(TColumnShard* self, TEvPrivate::TEvExport::TPtr& ev)
        : TBase(self)
        , Ev(ev)
        , TabletTxNo(++Self->TabletTxCounter)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_EXPORT; }

private:
    TEvPrivate::TEvExport::TPtr Ev;
    const ui32 TabletTxNo;
    THashMap<TString, THashSet<NOlap::TEvictedBlob>> BlobsToForget;

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxExportFinish[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }
};


bool TTxExportFinish::Execute(TTransactionContext& txc, const TActorContext&) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG(TxPrefix() << "execute" << TxSuffix());

    txc.DB.NoMoreReadsForTx();
    //NIceDb::TNiceDb db(txc.DB);

    auto& msg = *Ev->Get();
    auto status = msg.Status;

    {
        TBlobManagerDb blobManagerDb(txc.DB);

        for (auto& [blob, externId] : msg.SrcToDstBlobs) {
            auto& blobId = blob;
            Y_VERIFY(blobId.IsDsBlob());
            Y_VERIFY(externId.IsS3Blob());
            bool dropped = false;

            if (!msg.Blobs.contains(blobId)) {
                Y_VERIFY(!msg.ErrorStrings.empty());
                continue; // not exported
            }

            // TODO: SELF_CACHED logic

            NOlap::TEvictedBlob evict{
                .State = EEvictState::EXTERN,
                .Blob = blobId,
                .ExternBlob = externId
            };
            bool present = Self->BlobManager->UpdateOneToOne(std::move(evict), blobManagerDb, dropped);

            // Delayed erase of evicted blob. Blob could be already deleted.
            if (present && !dropped) {
                LOG_S_INFO(TxPrefix() << "Blob exported '" << blobId.ToStringNew() << "'" << TxSuffix());
                Self->BlobManager->DeleteBlob(blobId, blobManagerDb);
                Self->IncCounter(COUNTER_BLOBS_ERASED);
                Self->IncCounter(COUNTER_BYTES_ERASED, blobId.BlobSize());
            } else if (present && dropped) {
                LOG_S_INFO(TxPrefix() << "Stale blob exported '" << blobId.ToStringNew() << "'" << TxSuffix());

                TEvictMetadata meta;
                evict = Self->BlobManager->GetDropped(blobId, meta);
                Y_VERIFY(evict.State == EEvictState::EXTERN);

                BlobsToForget[meta.GetTierName()].emplace(std::move(evict));
            } else {
                LOG_S_ERROR(TxPrefix() << "Unknown blob exported '" << blobId.ToStringNew() << "'" << TxSuffix());
            }
        }
    }

    if (status == NKikimrProto::OK) {
        Self->IncCounter(COUNTER_EXPORT_SUCCESS);
    } else {
        Self->IncCounter(COUNTER_EXPORT_FAIL);
    }

    return true;
}

void TTxExportFinish::Complete(const TActorContext& ctx) {
    Y_VERIFY(Ev);
    LOG_S_DEBUG(TxPrefix() << "complete" << TxSuffix());

    if (!BlobsToForget.empty()) {
        Self->ForgetBlobs(ctx, BlobsToForget);
    }
}


void TColumnShard::Handle(TEvPrivate::TEvExport::TPtr& ev, const TActorContext& ctx) {
    auto& msg = *ev->Get();
    auto status = msg.Status;

    ui64 exportNo = msg.ExportNo;
    auto& tierName = msg.TierName;

    if (status == NKikimrProto::UNKNOWN) {
        LOG_S_DEBUG("Export (write): id " << exportNo << " tier '" << tierName << "' at tablet " << TabletID());
        ExportBlobs(ctx, ev);
    } else if (status == NKikimrProto::ERROR && msg.Blobs.empty()) {
        LOG_S_WARN("Export (fail): id " << exportNo << " tier '" << tierName << "' error: "
            << ev->Get()->SerializeErrorsToString() << "' at tablet " << TabletID());
    } else {
        // There's no atomicity needed here. Allow partial export
        if (status == NKikimrProto::ERROR) {
            LOG_S_WARN("Export (partial): id " << exportNo << " tier '" << tierName << "' error: "
                << ev->Get()->SerializeErrorsToString() << "' at tablet " << TabletID());
        } else {
            LOG_S_DEBUG("Export (apply): id " << exportNo << " tier '" << tierName << "' at tablet " << TabletID());
        }
        Execute(new TTxExportFinish(this, ev), ctx);
    }
}

}
