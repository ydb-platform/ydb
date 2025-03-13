#include "fetcher.h"

namespace NKikimr::NOlap::NIndexes {

void TIndexFetcherLogic::DoStart(TReadActionsCollection& nextRead, NReader::NCommon::TFetchingResultContext& context) {
    TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
    StorageId = context.GetSource()->GetEntityStorageId(GetEntityId());
    auto indexChunks = context.GetSource()->GetStageData().GetPortionAccessor().GetIndexChunksPointers(IndexAddress.GetIndexId());
    for (auto&& i : indexChunks) {
        if (i->HasBlobData()) {
            TChunkOriginalData originalData(i->GetBlobDataVerified());
            Fetching.emplace_back(TIndexChunkFetching(StorageId, IndexAddress, originalData, IndexMeta->BuildHeader(originalData).DetachResult(), i->GetRecordsCount()));
        } else {
            TChunkOriginalData originalData(
                context.GetSource()->GetStageData().GetPortionAccessor().GetPortionInfo().RestoreBlobRange(i->GetBlobRangeVerified()));
            Fetching.emplace_back(TIndexChunkFetching(
                StorageId, IndexAddress, originalData, IndexMeta->BuildHeader(originalData).DetachResult(), i->GetRecordsCount()));
        }
    }
    std::shared_ptr<IBlobsReadingAction> reading;
    for (auto&& i : Fetching) {
        if (!i.GetResult().HasData()) {
            if (!reading) {
                reading = blobsAction.GetReading(StorageId);
            }
            reading->AddRange(i.GetResult().GetRangeVerified());
        }
    }
    if (reading) {
        nextRead.Add(reading);
    }
}

void TIndexFetcherLogic::DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
    if (blobs.IsEmpty()) {
        return;
    }
    TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
    std::vector<TBlobRange> ranges;
    for (auto&& r : Fetching) {
        r.FetchFrom(IndexMeta, StorageId, ranges, blobs);
    }
    if (ranges.size()) {
        auto reading = blobsAction.GetReading(StorageId);
        for (auto&& i : ranges) {
            reading->AddRange(i);
        }
        nextRead.Add(reading);
    }
}

void TIndexFetcherLogic::DoOnDataCollected(NReader::NCommon::TFetchingResultContext& context) {
    if (Fetching.empty()) {
        return;
    }
    std::vector<TString> data;
    const bool hasIndex = context.GetIndexes().HasIndex(IndexAddress.GetIndexId());

    for (auto&& i : Fetching) {
        data.emplace_back(i.GetResult().GetBlobData());
        if (!hasIndex) {
            context.GetIndexes().StartChunk(IndexAddress.GetIndexId(), IndexMeta, i.GetHeader(), i.GetRecordsCount());
        }
    }
    context.GetIndexes().AddData(DataAddress, IndexAddress, data);
}

}   // namespace NKikimr::NOlap::NIndexes
