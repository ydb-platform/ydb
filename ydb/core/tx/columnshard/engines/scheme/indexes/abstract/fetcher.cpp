#include "fetcher.h"

#include "stats.h"

#include <util/generic/scope.h>

namespace NKikimr::NOlap::NIndexes {

namespace {

std::shared_ptr<NScanStats::TOperation> StartIndexStats(
    NScanStats::TStep::EStepKind kind, const TString& name, const IIndexMeta& index, NReader::NCommon::IDataSource& source) {
    const auto& collector = NScanStats::CurrentCollector();
    if (!collector) {
        return {};
    }
    auto operation = collector->Start(kind, name, TString("Index ") + index.GetIndexName(), NScanStats::Current(), source.GetSourceId());
    if (operation) {
        operation->Update([&](NScanStats::TStep& record) {
            record.SetMetaStep("IndexFilter");
            record.SetCanonicalPath("");
            record.ClearIndexDescriptors();
            record.ClearSharedIndexInputs();
            const auto& resolver = *source.GetContext()->GetCommonContext()->GetResolver();
            AddIndexStatsDescriptor(record, index, [&](ui32 id) { return resolver.GetColumnName(id, false); });
            if (kind == NScanStats::TStep::FetchColumns || kind == NScanStats::TStep::Assemble) {
                auto* columns = kind == NScanStats::TStep::FetchColumns
                    ? record.MutableDetails()->MutableFetchColumns()->MutableColumns()
                    : record.MutableDetails()->MutableAssemble()->MutableColumns();
                for (const auto& column : record.GetIndexDescriptors(0).GetColumns()) {
                    columns->AddNames(column);
                }
            }
        });
    }
    return operation;
}

TString GetStorageIdForIndexChunk(
    const NReader::NCommon::IDataSource& source, const std::shared_ptr<IIndexMeta>& indexMeta, const std::optional<TBlobRange>& blobRange) {
    if (blobRange && blobRange->BlobId.GetDsGroup() == Max<ui32>()) {
        return source.GetPortionAccessor().GetPortionInfo().GetMeta().GetTierName();
    }
    return source.GetIndexStorageId(indexMeta->GetIndexId());
}

}   // namespace

void TIndexFetcherLogic::DoStart(TReadActionsCollection& nextRead, NReader::NCommon::TFetchingResultContext& context) {
    auto stats = StartIndexStats(NScanStats::TStep::FetchColumns, "FETCH_INDEX", *IndexMeta, context.GetSource());
    if (stats) {
        RuntimeIndexStats.emplace(stats, NScanStats::CurrentCollector());
    }
    NScanStats::TScope scope(stats);
    Y_DEFER { if (stats) { stats->Finish(); } };
    TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
    auto& source = context.GetSource();
    const auto& portionAccessor = source.GetPortionAccessor();
    auto indexChunks = portionAccessor.GetIndexChunksPointers(IndexMeta->GetIndexId());
    for (auto&& i : indexChunks) {
        if (i->HasBlobData()) {
            NScanStats::RecordDataSourceRead("Memory", "InlineIndex", i->GetBlobDataVerified().size(),
                NKqpProto::TColumnShardStepDetails::TStorageRead::index);
            TChunkOriginalData originalData(i->GetBlobDataVerified());
            const TString storageId = GetStorageIdForIndexChunk(source, IndexMeta, std::nullopt);
            FetchingStorageIds.emplace_back(storageId);
            Fetching.emplace_back(TIndexChunkFetching(
                storageId, IndexAddressesVector, originalData, IndexMeta->BuildHeader(originalData).DetachResult(), i->GetRecordsCount()));
        } else {
            const TBlobRange blobRange = portionAccessor.RestoreBlobRange(i->GetBlobRangeVerified());
            const TString storageId = GetStorageIdForIndexChunk(source, IndexMeta, blobRange);
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
            reading->AddRange(i, {}, NKqpProto::TColumnShardStepDetails::TStorageRead::index);
        }
        nextRead.Add(reading);
    }
}

void TIndexFetcherLogic::DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
    if (blobs.IsEmpty()) {
        return;
    }
    std::optional<NScanStats::TScope> indexContext;
    if (RuntimeIndexStats) {
        indexContext.emplace(*RuntimeIndexStats);
    }
    const auto& collector = NScanStats::CurrentCollector();
    auto stats = collector && RuntimeIndexStats
        ? collector->Start(NScanStats::TStep::FetchColumns, "FETCH_INDEX_BODY", TString("Index ") + IndexMeta->GetIndexName(), NScanStats::Current())
        : nullptr;
    if (stats) {
        stats->Update([](NScanStats::TStep& record) {
            record.SetCanonicalPath("");
            record.ClearSharedIndexInputs();
        });
    }
    NScanStats::TScope scope(stats);
    Y_DEFER { if (stats) { stats->Finish(); } };
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
            reading->AddRange(i, {}, NKqpProto::TColumnShardStepDetails::TStorageRead::index);
        }
        nextRead.Add(reading);
    }
}

TConclusionStatus TIndexFetcherLogic::DoOnDataCollected(NReader::NCommon::TFetchingResultContext& context) {
    if (Fetching.empty()) {
        return TConclusionStatus::Success();
    }
    auto stats = StartIndexStats(NScanStats::TStep::Assemble, "ASSEMBLE_INDEX", *IndexMeta, context.GetSource());
    NScanStats::TScope scope(stats);
    Y_DEFER { if (stats) { stats->Finish(); } };
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
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap::NIndexes
