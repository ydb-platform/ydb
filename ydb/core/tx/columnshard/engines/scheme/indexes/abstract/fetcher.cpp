#include "fetcher.h"

namespace NKikimr::NOlap::NIndexes {

namespace {

TString GetStorageIdForIndexChunk(const TPortionDataAccessor& portionAccessor, const TIndexInfo& indexInfo,
    const std::shared_ptr<IIndexMeta>& indexMeta, const std::optional<TBlobRange>& blobRange) {
    if (blobRange && blobRange->BlobId.GetDsGroup() == Max<ui32>()) {
        return portionAccessor.GetPortionInfo().GetMeta().GetTierName();
    }
    return portionAccessor.GetPortionInfo().GetIndexStorageId(indexMeta->GetIndexId(), indexInfo);
}

}   // namespace

void TIndexFetcherLogic::DoStart(TReadActionsCollection& nextRead, NReader::NCommon::TFetchingResultContext& context) {
    TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
    auto source = context.GetSource();
    const auto& portionAccessor = source->GetPortionAccessor();
    const auto& indexInfo = source->GetSourceSchema()->GetIndexInfo();
    auto indexChunks = portionAccessor.GetIndexChunksPointers(IndexMeta->GetIndexId());
    for (auto&& i : indexChunks) {
        if (i->HasBlobData()) {
            TChunkOriginalData originalData(i->GetBlobDataVerified());
            const TString storageId = GetStorageIdForIndexChunk(portionAccessor, indexInfo, IndexMeta, std::nullopt);
            FetchingStorageIds.emplace_back(storageId);
            Fetching.emplace_back(TIndexChunkFetching(
                storageId, IndexAddressesVector, originalData, IndexMeta->BuildHeader(originalData).DetachResult(), i->GetRecordsCount()));
        } else {
            const TBlobRange blobRange = portionAccessor.RestoreBlobRange(i->GetBlobRangeVerified());
            const TString storageId = GetStorageIdForIndexChunk(portionAccessor, indexInfo, IndexMeta, blobRange);
            FetchingStorageIds.emplace_back(storageId);
            TChunkOriginalData originalData(blobRange);
            Fetching.emplace_back(TIndexChunkFetching(
                storageId, IndexAddressesVector, originalData, IndexMeta->BuildHeader(originalData).DetachResult(), i->GetRecordsCount()));
        }
    }
    THashMap<TString, std::vector<TBlobRange>> rangesByStorage;
    AFL_VERIFY(Fetching.size() == FetchingStorageIds.size());
    for (ui32 idx = 0; idx < Fetching.size(); ++idx) {
        auto rangesLocal = Fetching[idx].GetResult().GetRangesToFetch();
        auto& ranges = rangesByStorage[FetchingStorageIds[idx]];
        ranges.insert(ranges.end(), rangesLocal.begin(), rangesLocal.end());
    }
    for (auto&& [storageId, ranges] : rangesByStorage) {
        std::sort(ranges.begin(), ranges.end());
        ranges.erase(std::unique(ranges.begin(), ranges.end()), ranges.end());
        if (ranges.empty()) {
            continue;
        }
        std::shared_ptr<IBlobsReadingAction> reading = blobsAction.GetReading(storageId);
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
    THashMap<TString, std::vector<TBlobRange>> rangesByStorage;
    {
        auto g = blobs.BuildGuard();
        AFL_VERIFY(Fetching.size() == FetchingStorageIds.size());
        for (ui32 idx = 0; idx < Fetching.size(); ++idx) {
            Fetching[idx].FetchFrom(IndexMeta, FetchingStorageIds[idx], g);
            auto rangesLocal = Fetching[idx].GetResult().GetRangesToFetch();
            auto& ranges = rangesByStorage[FetchingStorageIds[idx]];
            ranges.insert(ranges.end(), rangesLocal.begin(), rangesLocal.end());
        }
    }
    for (auto&& [storageId, ranges] : rangesByStorage) {
        std::sort(ranges.begin(), ranges.end());
        ranges.erase(std::unique(ranges.begin(), ranges.end()), ranges.end());
        if (ranges.empty()) {
            continue;
        }
        auto reading = blobsAction.GetReading(storageId);
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
