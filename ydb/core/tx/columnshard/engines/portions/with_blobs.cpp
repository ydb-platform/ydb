#include "with_blobs.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap {

void TPortionInfoWithBlobs::TBlobInfo::RestoreChunk(const TPortionInfoWithBlobs& owner, const std::shared_ptr<IPortionDataChunk>& chunk) {
    Y_ABORT_UNLESS(!ResultBlob);
    const TString& data = chunk->GetData();
    Size += data.size();
    auto address = chunk->GetChunkAddress();
    AFL_VERIFY(owner.GetPortionInfo().HasEntityAddress(address))("address", address.DebugString());
    AFL_VERIFY(Chunks.emplace(address, chunk).second)("address", address.DebugString());
    ChunksOrdered.emplace_back(chunk);
}

void TPortionInfoWithBlobs::TBlobInfo::AddChunk(TPortionInfoWithBlobs& owner, const std::shared_ptr<IPortionDataChunk>& chunk) {
    AFL_VERIFY(chunk);
    Y_ABORT_UNLESS(!ResultBlob);
    const TString& data = chunk->GetData();

    TBlobRangeLink16 bRange(Size, data.size());
    Size += data.size();

    Y_ABORT_UNLESS(Chunks.emplace(chunk->GetChunkAddress(), chunk).second);
    ChunksOrdered.emplace_back(chunk);

    chunk->AddIntoPortionBeforeBlob(bRange, owner.PortionInfo);
}

void TPortionInfoWithBlobs::TBlobInfo::RegisterBlobId(TPortionInfoWithBlobs& owner, const TUnifiedBlobId& blobId) {
    const TBlobRangeLink16::TLinkId idx = owner.PortionInfo.RegisterBlobId(blobId);
    for (auto&& i : Chunks) {
        owner.PortionInfo.RegisterBlobIdx(i.first, idx);
    }
}

void TPortionInfoWithBlobs::TBlobInfo::ExtractEntityChunks(const ui32 entityId, std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>>& resultMap) {
    const auto pred = [this, &resultMap, entityId](const std::shared_ptr<IPortionDataChunk>& chunk) {
        if (chunk->GetEntityId() == entityId) {
            resultMap.emplace(chunk->GetChunkAddress(), chunk);
            Chunks.erase(chunk->GetChunkAddress());
            return true;
        } else {
            return false;
        }
    };
    ChunksOrdered.erase(std::remove_if(ChunksOrdered.begin(), ChunksOrdered.end(), pred), ChunksOrdered.end());
}

std::shared_ptr<arrow::RecordBatch> TPortionInfoWithBlobs::GetBatch(const ISnapshotSchema::TPtr& data, const ISnapshotSchema& result, const std::set<std::string>& columnNames) const {
    Y_ABORT_UNLESS(data);
    if (columnNames.empty()) {
        if (!CachedBatch) {
            THashMap<TChunkAddress, TString> blobs;
            for (auto&& i : PortionInfo.Records) {
                blobs[i.GetAddress()] = GetBlobByRangeVerified(i.ColumnId, i.Chunk);
                Y_ABORT_UNLESS(blobs[i.GetAddress()].size() == i.BlobRange.Size);
            }
            CachedBatch = PortionInfo.AssembleInBatch(*data, result, blobs);
            Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(*CachedBatch, result.GetIndexInfo().GetReplaceKey()));
        }
        return *CachedBatch;
    } else if (CachedBatch) {
        std::vector<TString> columnNamesString;
        for (auto&& i : columnNames) {
            columnNamesString.emplace_back(i.data(), i.size());
        }
        auto result = NArrow::ExtractColumns(*CachedBatch, columnNamesString);
        Y_ABORT_UNLESS(result);
        return result;
    } else {
        auto filteredSchema = std::make_shared<TFilteredSnapshotSchema>(data, columnNames);
        THashMap<TChunkAddress, TString> blobs;
        for (auto&& i : PortionInfo.Records) {
            blobs[i.GetAddress()] = GetBlobByRangeVerified(i.ColumnId, i.Chunk);
            Y_ABORT_UNLESS(blobs[i.GetAddress()].size() == i.BlobRange.Size);
        }
        return PortionInfo.AssembleInBatch(*data, *filteredSchema, blobs);
    }
}

NKikimr::NOlap::TPortionInfoWithBlobs TPortionInfoWithBlobs::RestorePortion(const TPortionInfo& portion, NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo, const std::shared_ptr<IStoragesManager>& operators) {
    TPortionInfoWithBlobs result(portion);
    THashMap<TString, THashMap<TUnifiedBlobId, std::vector<std::shared_ptr<IPortionDataChunk>>>> records = result.PortionInfo.RestoreEntityChunks(blobs, indexInfo);
    for (auto&& [storageId, recordsByBlob] : records) {
        auto storage = operators->GetOperatorVerified(storageId);
        for (auto&& i : recordsByBlob) {
            auto builder = result.StartBlob(storage);
            for (auto&& d : i.second) {
                builder.RestoreChunk(d);
            }
        }
    }
    return result;
}

std::vector<NKikimr::NOlap::TPortionInfoWithBlobs> TPortionInfoWithBlobs::RestorePortions(const std::vector<TPortionInfo>& portions, NBlobOperations::NRead::TCompositeReadBlobs& blobs,
    const TVersionedIndex& tables, const std::shared_ptr<IStoragesManager>& operators) {
    std::vector<TPortionInfoWithBlobs> result;
    for (auto&& i : portions) {
        const auto schema = tables.GetSchema(i.GetMinSnapshot());
        result.emplace_back(RestorePortion(i, blobs, schema->GetIndexInfo(), operators));
    }
    return result;
}

NKikimr::NOlap::TPortionInfoWithBlobs TPortionInfoWithBlobs::BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
    std::shared_ptr<arrow::RecordBatch> batch, const ui64 granule, const TSnapshot& snapshot, const std::shared_ptr<IStoragesManager>& operators)
{
    TPortionInfoWithBlobs result = BuildByBlobs(std::move(chunks), TPortionInfo(granule, 0, snapshot), operators);
    result.InitBatchCached(batch);
    return result;
}

TPortionInfoWithBlobs TPortionInfoWithBlobs::BuildByBlobs(std::vector<TSplittedBlob>&& chunks, const TPortionInfo& basePortion,
    const std::shared_ptr<IStoragesManager>& operators) {
    TPortionInfoWithBlobs result(basePortion.CopyBeforeChunksRebuild());
    for (auto&& blob : chunks) {
        auto storage = operators->GetOperatorVerified(blob.GetGroupName());
        auto blobInfo = result.StartBlob(storage);
        for (auto&& chunk : blob.GetChunks()) {
            blobInfo.AddChunk(chunk);
        }
    }
    result.GetPortionInfo().ReorderChunks();
    return result;
}

std::vector<std::shared_ptr<IPortionDataChunk>> TPortionInfoWithBlobs::GetEntityChunks(const ui32 entityId) const {
    std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>> sortedChunks;
    for (auto&& b : GetBlobs()) {
        for (auto&& i : b.GetChunks()) {
            if (i.second->GetEntityId() == entityId) {
                sortedChunks.emplace(i.first, i.second);
            }
        }
    }
    std::vector<std::shared_ptr<IPortionDataChunk>> result;
    for (auto&& i : sortedChunks) {
        AFL_VERIFY(i.second->GetChunkIdx() == result.size())("idx", i.second->GetChunkIdx())("size", result.size());
        result.emplace_back(i.second);
    }
    return result;
}

bool TPortionInfoWithBlobs::ExtractColumnChunks(const ui32 columnId, std::vector<const TColumnRecord*>& records, std::vector<std::shared_ptr<IPortionDataChunk>>& chunks) {
    records = GetPortionInfo().GetColumnChunksPointers(columnId);
    if (records.empty()) {
        return false;
    }
    std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>> chunksMap;
    for (auto&& i : Blobs) {
        i.ExtractEntityChunks(columnId, chunksMap);
    }
    std::vector<std::shared_ptr<IPortionDataChunk>> chunksLocal;
    for (auto&& i : chunksMap) {
        Y_ABORT_UNLESS(i.first.GetColumnId() == columnId);
        Y_ABORT_UNLESS(i.first.GetChunk() == chunksLocal.size());
        chunksLocal.emplace_back(i.second);
    }
    std::swap(chunksLocal, chunks);
    return true;
}

void TPortionInfoWithBlobs::FillStatistics(const TIndexInfo& index) {
    NStatistics::TPortionStorage storage;
    for (auto&& i : index.GetStatistics()) {
        THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> data;
        for (auto&& entityId : i.second->GetEntityIds()) {
            data.emplace(entityId, GetEntityChunks(entityId));
        }
        i.second->FillStatisticsData(data, storage, index);
    }
    PortionInfo.SetStatisticsStorage(std::move(storage));
}

TPortionInfoWithBlobs TPortionInfoWithBlobs::SyncPortion(TPortionInfoWithBlobs&& source,
    const ISnapshotSchema::TPtr& from, const ISnapshotSchema::TPtr& to, const TString& targetTier, const std::shared_ptr<IStoragesManager>& storages,
    std::shared_ptr<NColumnShard::TSplitterCounters> counters) {
    if (from->GetVersion() == to->GetVersion() && targetTier == source.GetPortionInfo().GetTierNameDef(IStoragesManager::DefaultStorageId)) {
        return std::move(source);
    }
    NYDBTest::TControllers::GetColumnShardController()->OnPortionActualization(source.PortionInfo);
    auto pages = source.PortionInfo.BuildPages();
    std::vector<ui32> pageSizes;
    for (auto&& p : pages) {
        pageSizes.emplace_back(p.GetRecordsCount());
    }
    THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> columnChunks;
    for (auto&& i : source.Blobs) {
        for (auto&& c : i.GetChunks()) {
            columnChunks[c.first.GetColumnId()].emplace_back(c.second);
        }
    }

    THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> entityChunksNew;
    for (auto&& i : to->GetIndexInfo().GetColumnIds()) {
        auto it = columnChunks.find(i);
        std::vector<std::shared_ptr<IPortionDataChunk>> newChunks;
        if (it != columnChunks.end()) {
            newChunks = to->GetIndexInfo().ActualizeColumnData(it->second, from->GetIndexInfo(), i);
        } else {
            newChunks = to->GetIndexInfo().MakeEmptyChunks(i, pageSizes, to->GetIndexInfo().GetColumnFeaturesVerified(i));
        }
        AFL_VERIFY(entityChunksNew.emplace(i, std::move(newChunks)).second);
    }

    for (auto&& i : to->GetIndexInfo().GetIndexes()) {
        if (from->GetIndexInfo().HasIndexId(i.first)) {
            continue;
        }
        to->GetIndexInfo().AppendIndex(entityChunksNew, i.first);
    }

    auto schemaTo = std::make_shared<TDefaultSchemaDetails>(to, std::make_shared<TSerializationStats>());
    TGeneralSerializedSlice slice(entityChunksNew, schemaTo, counters);
    const NSplitter::TEntityGroups groups = to->GetIndexInfo().GetEntityGroupsByStorageId(targetTier, *storages);
    TPortionInfoWithBlobs result = TPortionInfoWithBlobs::BuildByBlobs(slice.GroupChunksByBlobs(groups), source.PortionInfo, storages);
    result.GetPortionInfo().SetMinSnapshot(to->GetSnapshot());
    result.GetPortionInfo().MutableMeta().SetTierName(targetTier);

    NStatistics::TPortionStorage storage;
    for (auto&& i : to->GetIndexInfo().GetStatistics()) {
        auto it = from->GetIndexInfo().GetStatistics().find(i.first);
        if (it != from->GetIndexInfo().GetStatistics().end()) {
            i.second->CopyData(it->second.GetCursorVerified(), source.PortionInfo.GetMeta().GetStatisticsStorage(), storage);
        } else {
            i.second->FillStatisticsData(entityChunksNew, storage, to->GetIndexInfo());
        }
    }
    result.PortionInfo.MutableMeta().ResetStatisticsStorage(std::move(storage));
    return result;
}

}
