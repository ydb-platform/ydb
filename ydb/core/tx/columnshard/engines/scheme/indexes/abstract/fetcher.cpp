#include "fetcher.h"

namespace NKikimr::NOlap::NIndexes {

void TIndexFetcherLogic::DoStart(TReadActionsCollection& nextRead, NReader::NCommon::TFetchingResultContext& context) {
    TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
    StorageId = context.GetSource()->GetEntityStorageId(GetEntityId());
    auto indexChunks = context.GetSource()->GetPortionAccessor().GetIndexChunksPointers(IndexMeta->GetIndexId());
    for (auto&& i : indexChunks) {
        if (i->HasBlobData()) {
            TChunkOriginalData originalData(i->GetBlobDataVerified());
            Fetching.emplace_back(TIndexChunkFetching(
                StorageId, IndexAddressesVector, originalData, IndexMeta->BuildHeader(originalData).DetachResult(), i->GetRecordsCount()));
        } else {
            TChunkOriginalData originalData(
                context.GetSource()->GetPortionAccessor().RestoreBlobRange(i->GetBlobRangeVerified()));
            Fetching.emplace_back(TIndexChunkFetching(
                StorageId, IndexAddressesVector, originalData, IndexMeta->BuildHeader(originalData).DetachResult(), i->GetRecordsCount()));
        }
    }
    std::vector<TBlobRange> ranges;
    for (auto&& i : Fetching) {
        auto rangesLocal = i.GetResult().GetRangesToFetch();
        ranges.insert(ranges.end(), rangesLocal.begin(), rangesLocal.end());
    }
    ranges.erase(std::unique(ranges.begin(), ranges.end()), ranges.end());
    if (ranges.size()) {
        std::shared_ptr<IBlobsReadingAction> reading = blobsAction.GetReading(StorageId);
        for (auto&& i : ranges) {
            reading->AddRange(i);
        }
        nextRead.Add(reading);
    }
}

void TIndexFetcherLogic::DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
    if (blobs.IsEmpty()) {
        return;
    }
    TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
    std::vector<TBlobRange> ranges;
    {
        auto g = blobs.BuildGuard();
        for (auto&& r : Fetching) {
            r.FetchFrom(IndexMeta, StorageId, g);
            auto rangesLocal = r.GetResult().GetRangesToFetch();
            ranges.insert(ranges.end(), rangesLocal.begin(), rangesLocal.end());
        }
    }
    ranges.erase(std::unique(ranges.begin(), ranges.end()), ranges.end());
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
    const bool hasIndex = context.GetIndexes().HasIndex(IndexMeta->GetIndexId());
    std::vector<std::vector<TString>> data;
    data.resize(IndexAddressesVector.size());
    for (auto&& i : Fetching) {
        if (!hasIndex) {
            context.GetIndexes().StartChunk(IndexMeta->GetIndexId(), IndexMeta, i.GetHeader(), i.GetRecordsCount());
        }
        ui32 idx = 0;
        for (auto&& c : i.GetResult().GetColumnsFetching()) {
            AFL_VERIFY(idx < data.size());
            data[idx].emplace_back(c.GetBlobData());
            ++idx;
        }
    }
    for (ui32 idx = 0; idx < IndexAddressesVector.size(); ++idx) {
        auto it = IndexAddresses.find(IndexAddressesVector[idx]);
        AFL_VERIFY(it != IndexAddresses.end());
        for (auto&& originalDataAddress : it->second) {
            AFL_VERIFY(idx < data.size());
            context.GetIndexes().AddData(originalDataAddress, IndexAddressesVector[idx], data[idx]);
        }
    }
}

}   // namespace NKikimr::NOlap::NIndexes
