#include "read_with_blobs.h"
#include "write_with_blobs.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/filtered_scheme.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>

namespace NKikimr::NOlap {

void TReadPortionInfoWithBlobs::RestoreChunk(const std::shared_ptr<IPortionDataChunk>& chunk) {
    auto address = chunk->GetChunkAddressVerified();
    AFL_VERIFY(GetPortionInfo().HasEntityAddress(address))("address", address.DebugString());
    AFL_VERIFY(Chunks.emplace(address, chunk).second)("address", address.DebugString());
}

std::shared_ptr<arrow::RecordBatch> TReadPortionInfoWithBlobs::GetBatch(const ISnapshotSchema::TPtr& data, const ISnapshotSchema& result, const std::set<std::string>& columnNames) const {
    Y_ABORT_UNLESS(data);
    if (columnNames.empty()) {
        if (!CachedBatch) {
            THashMap<TChunkAddress, TString> blobs;
            for (auto&& i : PortionInfo.Records) {
                blobs[i.GetAddress()] = GetBlobByAddressVerified(i.ColumnId, i.Chunk);
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
            blobs[i.GetAddress()] = GetBlobByAddressVerified(i.ColumnId, i.Chunk);
            Y_ABORT_UNLESS(blobs[i.GetAddress()].size() == i.BlobRange.Size);
        }
        return PortionInfo.AssembleInBatch(*data, *filteredSchema, blobs);
    }
}

NKikimr::NOlap::TReadPortionInfoWithBlobs TReadPortionInfoWithBlobs::RestorePortion(const TPortionInfo& portion, NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) {
    TReadPortionInfoWithBlobs result(portion);
    THashMap<TString, THashMap<TUnifiedBlobId, std::vector<std::shared_ptr<IPortionDataChunk>>>> records = result.PortionInfo.RestoreEntityChunks(blobs, indexInfo);
    for (auto&& [storageId, recordsByBlob] : records) {
        for (auto&& i : recordsByBlob) {
            for (auto&& d : i.second) {
                result.RestoreChunk(d);
            }
        }
    }
    return result;
}

std::vector<NKikimr::NOlap::TReadPortionInfoWithBlobs> TReadPortionInfoWithBlobs::RestorePortions(const std::vector<TPortionInfo>& portions, NBlobOperations::NRead::TCompositeReadBlobs& blobs,
    const TVersionedIndex& tables) {
    std::vector<TReadPortionInfoWithBlobs> result;
    for (auto&& i : portions) {
        const auto schema = i.GetSchema(tables);
        result.emplace_back(RestorePortion(i, blobs, schema->GetIndexInfo()));
    }
    return result;
}

std::vector<std::shared_ptr<IPortionDataChunk>> TReadPortionInfoWithBlobs::GetEntityChunks(const ui32 entityId) const {
    std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>> sortedChunks;
    for (auto&& i : Chunks) {
        if (i.second->GetEntityId() == entityId) {
            sortedChunks.emplace(i.first, i.second);
        }
    }
    std::vector<std::shared_ptr<IPortionDataChunk>> result;
    for (auto&& i : sortedChunks) {
        AFL_VERIFY(i.second->GetChunkIdxVerified() == result.size())("idx", i.second->GetChunkIdxVerified())("size", result.size());
        result.emplace_back(i.second);
    }
    return result;
}

bool TReadPortionInfoWithBlobs::ExtractColumnChunks(const ui32 entityId, std::vector<const TColumnRecord*>& records, std::vector<std::shared_ptr<IPortionDataChunk>>& chunks) {
    records = GetPortionInfo().GetColumnChunksPointers(entityId);
    if (records.empty()) {
        return false;
    }
    std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>> chunksMap;
    for (auto it = Chunks.begin(); it != Chunks.end();) {
        if (it->first.GetEntityId() == entityId) {
            chunksMap.emplace(it->first, std::move(it->second));
            it = Chunks.erase(it);
        } else {
            ++it;
        }
    }
    std::vector<std::shared_ptr<IPortionDataChunk>> chunksLocal;
    for (auto&& i : chunksMap) {
        Y_ABORT_UNLESS(i.first.GetColumnId() == entityId);
        Y_ABORT_UNLESS(i.first.GetChunk() == chunksLocal.size());
        chunksLocal.emplace_back(i.second);
    }
    std::swap(chunksLocal, chunks);
    return true;
}

std::optional<TWritePortionInfoWithBlobs> TReadPortionInfoWithBlobs::SyncPortion(TReadPortionInfoWithBlobs&& source,
    const ISnapshotSchema::TPtr& from, const ISnapshotSchema::TPtr& to, const TString& targetTier, const std::shared_ptr<IStoragesManager>& storages,
    std::shared_ptr<NColumnShard::TSplitterCounters> counters) {
    if (from->GetVersion() == to->GetVersion() && targetTier == source.GetPortionInfo().GetTierNameDef(IStoragesManager::DefaultStorageId)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "we don't need sync portion");
        return {};
    }
    NYDBTest::TControllers::GetColumnShardController()->OnPortionActualization(source.PortionInfo);
    auto pages = source.PortionInfo.BuildPages();
    std::vector<ui32> pageSizes;
    for (auto&& p : pages) {
        pageSizes.emplace_back(p.GetRecordsCount());
    }
    THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> columnChunks;
    for (auto&& c : source.Chunks) {
        auto& chunks = columnChunks[c.first.GetColumnId()];
        AFL_VERIFY(c.first.GetChunkIdx() == chunks.size());
        chunks.emplace_back(c.second);
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
    TPortionInfoConstructor constructor(source.PortionInfo, false, true);
    constructor.SetMinSnapshotDeprecated(to->GetSnapshot());
    constructor.SetSchemaVersion(to->GetVersion());
    constructor.MutableMeta().ResetTierName(targetTier);

    NStatistics::TPortionStorage storage;
    for (auto&& i : to->GetIndexInfo().GetStatisticsByName()) {
        auto it = from->GetIndexInfo().GetStatisticsByName().find(i.first);
        if (it != from->GetIndexInfo().GetStatisticsByName().end()) {
            i.second->CopyData(it->second.GetCursorVerified(), source.PortionInfo.GetMeta().GetStatisticsStorage(), storage);
        } else {
            i.second->FillStatisticsData(entityChunksNew, storage, to->GetIndexInfo());
        }
    }
    constructor.MutableMeta().ResetStatisticsStorage(std::move(storage));

    TWritePortionInfoWithBlobs result = TWritePortionInfoWithBlobs::BuildByBlobs(slice.GroupChunksByBlobs(groups), std::move(constructor), storages);
    return result;
}

const TString& TReadPortionInfoWithBlobs::GetBlobByAddressVerified(const ui32 columnId, const ui32 chunkId) const {
    auto it = Chunks.find(TChunkAddress(columnId, chunkId));
    AFL_VERIFY(it != Chunks.end())("column_id", columnId)("chunk_idx", chunkId);
    return it->second->GetData();
}

}
