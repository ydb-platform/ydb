#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"
#include "blob_manager_db.h"

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxReadBlobRanges : public TTransactionBase<TColumnShard> {
public:
    TTxReadBlobRanges(TColumnShard* self, TEvColumnShard::TEvReadBlobRanges::TPtr& ev)
        : TTransactionBase<TColumnShard>(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_READ_BLOB_RANGES; }

private:
    TEvColumnShard::TEvReadBlobRanges::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvReadBlobRangesResult> Result;
};


// Returns false in case of page fault
bool TryReadValue(NIceDb::TNiceDb& db, const TString& key, TString& value, ui32& readStatus) {
    auto rowset = db.Table<Schema::SmallBlobs>().Key(key).Select<Schema::SmallBlobs::Data>();
    if (!rowset.IsReady()) {
        return false;
    }

    if (rowset.IsValid()) {
        readStatus = NKikimrProto::EReplyStatus::OK;
        value = rowset.GetValue<Schema::SmallBlobs::Data>();
    } else {
        readStatus = NKikimrProto::EReplyStatus::NODATA;
        value.clear();
    }
    return true;
}

bool TTxReadBlobRanges::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_VERIFY(Ev);
    auto& record = Ev->Get()->Record;
    LOG_S_DEBUG("TTxReadBlobRanges.Execute at tablet " << Self->TabletID()<< " : " << record);

    Result = std::make_unique<TEvColumnShard::TEvReadBlobRangesResult>(Self->TabletID());

    NIceDb::TNiceDb db(txc.DB);

    ui64 successCount = 0;
    ui64 errorCount = 0;
    ui64 byteCount = 0;
    for (const auto& range : record.GetBlobRanges()) {
        auto blobId = range.GetBlobId();

        TString blob;
        ui32 status = NKikimrProto::EReplyStatus::NODATA;
        if (!TryReadValue(db, blobId, blob, status)) {
            return false; // Page fault
        }

        if (status == NKikimrProto::EReplyStatus::NODATA) {
            // If the value wasn't found by string key then try to parse the key as small blob id
            // and try lo lookup by this id serialized in the old format and in the new format
            TString error;
            NOlap::TUnifiedBlobId smallBlobId = NOlap::TUnifiedBlobId::ParseFromString(blobId, nullptr, error);

            if (smallBlobId.IsValid()) {
                if (!TryReadValue(db, smallBlobId.ToStringNew(), blob, status)) {
                    return false; // Page fault
                }

                if (status == NKikimrProto::EReplyStatus::NODATA &&
                    !TryReadValue(db, smallBlobId.ToStringLegacy(), blob, status))
                {
                    return false; // Page fault
                }
            }
        }

        auto* res = Result->Record.AddResults();
        res->MutableBlobRange()->CopyFrom(range);
        if (status == NKikimrProto::EReplyStatus::OK) {
            if (range.GetOffset() + range.GetSize() <= blob.size()) {
                res->SetData(blob.substr(range.GetOffset(), range.GetSize()));
                byteCount += range.GetSize();
            } else {
                LOG_S_NOTICE("TTxReadBlobRanges.Execute at tablet " << Self->TabletID()
                    << " the requested range " << range << " is outside blob data, blob size << " << blob.size());
                status = NKikimrProto::EReplyStatus::ERROR;
            }
        }
        res->SetStatus(status);
        if (status == NKikimrProto::EReplyStatus::OK) {
            ++successCount;
        } else {
            ++errorCount;
        }
    }

    // Sending result right away without waiting for Complete()
    // It is ok because the blob ids that were requested can only be known
    // to the caller if they have been already committed.
    ctx.Send(Ev->Sender, Result.release(), 0, Ev->Cookie);

    Self->IncCounter(COUNTER_SMALL_BLOB_READ_SUCCESS, successCount);
    Self->IncCounter(COUNTER_SMALL_BLOB_READ_ERROR, errorCount);
    Self->IncCounter(COUNTER_SMALL_BLOB_READ_BYTES, byteCount);

    return true;
}

void TTxReadBlobRanges::Complete(const TActorContext& ctx) {
    Y_UNUSED(ctx);
    LOG_S_DEBUG("TTxReadBlobRanges.Complete at tablet " << Self->TabletID());
}

static std::unique_ptr<TEvColumnShard::TEvReadBlobRangesResult>
MakeErrorResponse(const TEvColumnShard::TEvReadBlobRanges& msg, ui64 tabletId, ui32 status) {
    auto result = std::make_unique<TEvColumnShard::TEvReadBlobRangesResult>(tabletId);
    for (const auto& range : msg.Record.GetBlobRanges()) {
        auto* res = result->Record.AddResults();
        res->MutableBlobRange()->CopyFrom(range);
        res->SetStatus(status);
    }
    return result;
}

void TColumnShard::Handle(TEvColumnShard::TEvReadBlobRanges::TPtr& ev, const TActorContext& ctx) {
    auto& msg = *ev->Get();

    LOG_S_DEBUG("Read blob ranges at tablet " << TabletID() << msg.Record);

    if (msg.BlobRanges.empty()) {
        TBlobGroupSelector dsGroupSelector(Info());
        TString errString;
        msg.RestoreFromProto(&dsGroupSelector, errString);
        Y_VERIFY_S(errString.empty(), errString);
    }

    std::optional<TUnifiedBlobId> evictedBlobId;
    bool isSmall = false;
    bool isFallback = false;
    bool isOther = false;
    for (const auto& range : msg.BlobRanges) {
        auto& blobId = range.BlobId;
        if (blobId.IsSmallBlob()) {
            isSmall = true;
        } else if (blobId.IsDsBlob()) {
            isFallback = true;
            if (evictedBlobId) {
                // Can read only one blobId at a time (but multiple ranges from it)
                Y_VERIFY(evictedBlobId == blobId);
            } else {
                evictedBlobId = blobId;
            }
        } else {
            isOther = true;
        }
    }

    Y_VERIFY(isSmall != isFallback && !isOther);

    if (isSmall) {
        Execute(new TTxReadBlobRanges(this, ev), ctx);
    } else if (isFallback) {
        Y_VERIFY(evictedBlobId->IsValid());

        NKikimrTxColumnShard::TEvictMetadata meta;
        auto evicted = BlobManager->GetEvicted(*evictedBlobId, meta);

        if (!evicted.Blob.IsValid()) {
            evicted = BlobManager->GetDropped(*evictedBlobId, meta);
        }

        if (!evicted.Blob.IsValid() || !evicted.ExternBlob.IsValid()) {
            LOG_S_NOTICE("No data for blobId " << evictedBlobId->ToStringNew() << " at tablet " << TabletID());
            auto result = MakeErrorResponse(msg, TabletID(), NKikimrProto::EReplyStatus::NODATA);
            ctx.Send(ev->Sender, result.release(), 0, ev->Cookie);
            return;
        }

        TString tierName = meta.GetTierName();
        Y_VERIFY_S(!tierName.empty(), evicted.ToString());

        if (!GetExportedBlob(ctx, ev->Sender, ev->Cookie, tierName, std::move(evicted), std::move(msg.BlobRanges))) {
            auto result = MakeErrorResponse(msg, TabletID(), NKikimrProto::EReplyStatus::ERROR);
            ctx.Send(ev->Sender, result.release(), 0, ev->Cookie);
        }
    }
}

}
