#include "compacted_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>


namespace NKikimr::NOlap {

TCompactedWriteController::TBlobsConstructor::TBlobsConstructor(TCompactedWriteController& owner, bool blobGrouppingEnabled)
    : Owner(owner)
    , IndexChanges(*Owner.WriteIndexEv->IndexChanges)
    , Blobs(Owner.WriteIndexEv->Blobs)
    , BlobGrouppingEnabled(blobGrouppingEnabled)
    , CacheData(Owner.WriteIndexEv->CacheData)
    , EvictionFlag(IndexChanges.PortionsToEvict.size() > 0)
{
    LastPortion = IndexChanges.AppendedPortions.size() + IndexChanges.PortionsToEvict.size();
    Y_VERIFY(Blobs.size() > 0);
}

const TString& TCompactedWriteController::TBlobsConstructor::GetBlob() const {
    return AccumulatedBlob;
}

bool TCompactedWriteController::TBlobsConstructor::RegisterBlobId(const TUnifiedBlobId& blobId) {
    Y_VERIFY(AccumulatedBlob.size() > 0);
    Y_VERIFY(RecordsInBlob.size() > 0);

    auto& portionInfo = PortionUpdates.back();
    LOG_S_TRACE("Write Index Blob " << blobId << " with " << RecordsInBlob.size() << " records");
    for (const auto& rec : RecordsInBlob) {
        size_t i = rec.first;
        const TString& recData = rec.second;
        auto& blobRange = portionInfo.Records[i].BlobRange;
        blobRange.BlobId = blobId;
        Y_VERIFY(blobRange.Offset + blobRange.Size <= AccumulatedBlob.size());
        Y_VERIFY(blobRange.Size == recData.size());

        if (CacheData) {
            // Save original (non-accumulted) blobs with the corresponding TBlobRanges in order to
            // put them into cache at commit time
            Owner.WriteIndexEv->IndexChanges->Blobs[blobRange] = recData;
        }
    }
    return true;
}

IBlobConstructor::EStatus TCompactedWriteController::TBlobsConstructor::BuildNext() {
    AccumulatedBlob.clear();
    RecordsInBlob.clear();

    if (EvictionFlag && CurrentPortionRecord == 0) {
        // Skip portions without data changes
        for (; CurrentPortion < LastPortion; ++CurrentPortion) {
            if (IndexChanges.PortionsToEvict[CurrentPortion].second.DataChanges) {
                break;
            }
            PortionUpdates.push_back(GetPortionInfo(CurrentPortion));
        }
    }

    if (CurrentPortion == LastPortion) {
        Y_VERIFY(CurrentBlob == Blobs.size());
        return EStatus::Finished;
    }

    const auto& portionInfo = GetPortionInfo(CurrentPortion);
    if (CurrentPortionRecord == 0) {
        PortionUpdates.push_back(portionInfo);
    }
    NOlap::TPortionInfo& newPortionInfo = PortionUpdates.back();

    const auto& records = portionInfo.Records;
    for (; CurrentPortionRecord < records.size(); ++CurrentPortionRecord, ++CurrentBlob) {
        Y_VERIFY(CurrentBlob < Blobs.size());
        const TString& currentBlob = Blobs[CurrentBlob];
        Y_VERIFY(currentBlob.size());

        if ((!AccumulatedBlob.empty() && AccumulatedBlob.size() + currentBlob.size() > NColumnShard::TLimits::GetMaxBlobSize()) ||
            (AccumulatedBlob.size() && !BlobGrouppingEnabled))
        {
            return EStatus::Ok;
        }

        // Accumulate data chunks into a single blob and save record indices of these chunks
        newPortionInfo.Records[CurrentPortionRecord].BlobRange.Offset = AccumulatedBlob.size();
        newPortionInfo.Records[CurrentPortionRecord].BlobRange.Size = currentBlob.size();
        AccumulatedBlob.append(currentBlob);
        RecordsInBlob.emplace_back(CurrentPortionRecord, currentBlob);
    }

    ++CurrentPortion;
    CurrentPortionRecord = 0;
    return AccumulatedBlob.empty() ? EStatus::Finished : EStatus::Ok;
}

const NOlap::TPortionInfo& TCompactedWriteController::TBlobsConstructor::GetPortionInfo(const ui64 index) const {
    if (EvictionFlag) {
        Y_VERIFY(index < IndexChanges.PortionsToEvict.size());
        return IndexChanges.PortionsToEvict[index].first;
    } else {
        Y_VERIFY(index < IndexChanges.AppendedPortions.size());
        return IndexChanges.AppendedPortions[index];
    }
}

TCompactedWriteController::TCompactedWriteController(const TActorId& dstActor, TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeEv, bool blobGrouppingEnabled)
    : WriteIndexEv(writeEv)
    , BlobConstructor(std::make_shared<TBlobsConstructor>(*this, blobGrouppingEnabled))
    , DstActor(dstActor)
{}

void TCompactedWriteController::DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) {
    WriteIndexEv->PutResult = putResult;
    const auto& indexChanges = *WriteIndexEv->IndexChanges;

    for (ui64 index = 0; index < BlobConstructor->GetPortionUpdates().size(); ++index) {
        const auto& portionInfo = BlobConstructor->GetPortionUpdates()[index];
        if (BlobConstructor->IsEviction()) {
            Y_VERIFY(index < indexChanges.PortionsToEvict.size());
            WriteIndexEv->IndexChanges->PortionsToEvict[index].first = portionInfo;
        } else {
            Y_VERIFY(index < indexChanges.AppendedPortions.size());
            WriteIndexEv->IndexChanges->AppendedPortions[index] = portionInfo;
        }
    }
    ctx.Send(DstActor, WriteIndexEv.Release());
}

}
