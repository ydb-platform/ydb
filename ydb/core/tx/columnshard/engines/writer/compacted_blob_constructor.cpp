#include "compacted_blob_constructor.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blob.h>


namespace NKikimr::NOlap {

TCompactedBlobsConstructor::TCompactedBlobsConstructor(TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeIndexEv, bool blobGrouppingEnabled)
    : WriteIndexEv(writeIndexEv)
    , IndexChanges(*WriteIndexEv->IndexChanges)
    , Blobs(WriteIndexEv->Blobs)
    , BlobGrouppingEnabled(blobGrouppingEnabled)
    , CacheData(WriteIndexEv->CacheData)
{
    auto indexChanges = WriteIndexEv->IndexChanges;
    LastPortion = indexChanges->AppendedPortions.size() + indexChanges->PortionsToEvict.size();
    Y_VERIFY(Blobs.size() > 0);
}

const TString& TCompactedBlobsConstructor::GetBlob() const {
    return AccumulatedBlob;
}

bool TCompactedBlobsConstructor::RegisterBlobId(const TUnifiedBlobId& blobId) {
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
            WriteIndexEv->IndexChanges->Blobs[blobRange] = recData;
        }
    }
    return true;
}

IBlobConstructor::EStatus TCompactedBlobsConstructor::BuildNext(NColumnShard::TUsage& resourceUsage, const TAppData& /*appData*/) {
    Y_UNUSED(resourceUsage);
    if (CurrentPortion == LastPortion) {
        Y_VERIFY(CurrentBlob == Blobs.size());
        return EStatus::Finished;
    }

    AccumulatedBlob.clear();
    RecordsInBlob.clear();

    if (CurrentPortionRecord == 0) {
        PortionUpdates.push_back(GetPortionInfo(CurrentPortion));
        // There could be eviction mix between normal eviction and eviction without data changes
        // TODO: better portions to blobs mathching
        const bool eviction = IndexChanges.PortionsToEvict.size() > 0;
        if (eviction) {
            while (CurrentPortion < LastPortion && !IndexChanges.PortionsToEvict[CurrentPortion].second.DataChanges) {
                ++CurrentPortion;
                PortionUpdates.push_back(GetPortionInfo(CurrentPortion));
                continue;
            }
            if (CurrentPortion == LastPortion) {
                return EStatus::Finished;
            }
        }
    }

    auto& portionInfo = GetPortionInfo(CurrentPortion);
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

TAutoPtr<IEventBase> TCompactedBlobsConstructor::BuildResult(NKikimrProto::EReplyStatus status, NColumnShard::TBlobBatch&& blobBatch, THashSet<ui32>&& yellowMoveChannels, THashSet<ui32>&& yellowStopChannels, const NColumnShard::TUsage& resourceUsage) {
    for (ui64 index = 0; index < PortionUpdates.size(); ++index) {
        const auto& portionInfo = PortionUpdates[index];
        const bool eviction = IndexChanges.PortionsToEvict.size() > 0;
        if (eviction) {
            Y_VERIFY(index < IndexChanges.PortionsToEvict.size());
            WriteIndexEv->IndexChanges->PortionsToEvict[index].first = portionInfo;
        } else {
            Y_VERIFY(index < IndexChanges.AppendedPortions.size());
            WriteIndexEv->IndexChanges->AppendedPortions[index] = portionInfo;
        }
    }

    WriteIndexEv->ResourceUsage.Add(resourceUsage);
    WriteIndexEv->SetPutStatus(status, std::move(yellowMoveChannels), std::move(yellowStopChannels));
    WriteIndexEv->BlobBatch = std::move(blobBatch);
    return WriteIndexEv.Release();
}

const NOlap::TPortionInfo& TCompactedBlobsConstructor::GetPortionInfo(const ui64 index) const {
    bool eviction = IndexChanges.PortionsToEvict.size() > 0;
    if (eviction) {
        Y_VERIFY(index < IndexChanges.PortionsToEvict.size());
        return IndexChanges.PortionsToEvict[index].first;
    } else {
        Y_VERIFY(index < IndexChanges.AppendedPortions.size());
        return IndexChanges.AppendedPortions[index];
    }
}

}
