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

std::shared_ptr<NArrow::TGeneralContainer> TReadPortionInfoWithBlobs::RestoreBatch(
    const ISnapshotSchema& data, const ISnapshotSchema& resultSchema, const std::set<ui32>& seqColumns) const {
    THashMap<TChunkAddress, TString> blobs;
    for (auto&& i : PortionInfo.Records) {
        blobs[i.GetAddress()] = GetBlobByAddressVerified(i.ColumnId, i.Chunk);
        Y_ABORT_UNLESS(blobs[i.GetAddress()].size() == i.BlobRange.Size);
    }
    return PortionInfo.PrepareForAssemble(data, resultSchema, blobs).AssembleToGeneralContainer(seqColumns);
}

NKikimr::NOlap::TReadPortionInfoWithBlobs TReadPortionInfoWithBlobs::RestorePortion(const TPortionInfo& portion, NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) {
    TReadPortionInfoWithBlobs result(portion);
    THashMap<TString, THashMap<TChunkAddress, std::shared_ptr<IPortionDataChunk>>> records = result.PortionInfo.RestoreEntityChunks(blobs, indexInfo);
    for (auto&& [storageId, chunksByAddress] : records) {
        for (auto&& [_, chunk] : chunksByAddress) {
            result.RestoreChunk(chunk);
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
    std::vector<std::shared_ptr<IPortionDataChunk>> chunksLocal;
    for (auto it = Chunks.begin(); it != Chunks.end();) {
        if (it->first.GetEntityId() == entityId) {
            AFL_VERIFY(chunksLocal.empty() || chunksLocal.back()->GetChunkAddressVerified() < it->second->GetChunkAddressVerified());
            chunksLocal.emplace_back(std::move(it->second));
            it = Chunks.erase(it);
        } else {
            ++it;
        }
    }
    std::swap(chunksLocal, chunks);
    return true;
}

std::optional<TWritePortionInfoWithBlobsResult> TReadPortionInfoWithBlobs::SyncPortion(TReadPortionInfoWithBlobs&& source,
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
        }
        AFL_VERIFY(entityChunksNew.emplace(i, std::move(newChunks)).second);
    }

    TPortionInfoConstructor constructor(source.PortionInfo, false, true);
    constructor.SetMinSnapshotDeprecated(to->GetSnapshot());
    constructor.SetSchemaVersion(to->GetVersion());
    constructor.MutableMeta().ResetTierName(targetTier);

    TIndexInfo::TSecondaryData secondaryData;
    secondaryData.MutableExternalData() = entityChunksNew;
    for (auto&& i : to->GetIndexInfo().GetIndexes()) {
        to->GetIndexInfo().AppendIndex(entityChunksNew, i.first, storages, secondaryData).Validate();
    }

    const NSplitter::TEntityGroups groups = to->GetIndexInfo().GetEntityGroupsByStorageId(targetTier, *storages);
    auto schemaTo = std::make_shared<TDefaultSchemaDetails>(to, std::make_shared<TSerializationStats>());
    TGeneralSerializedSlice slice(secondaryData.GetExternalData(), schemaTo, counters);

    return TWritePortionInfoWithBlobsConstructor::BuildByBlobs(
        slice.GroupChunksByBlobs(groups), secondaryData.GetSecondaryInplaceData(), std::move(constructor), storages);
}

const TString& TReadPortionInfoWithBlobs::GetBlobByAddressVerified(const ui32 columnId, const ui32 chunkId) const {
    auto it = Chunks.find(TChunkAddress(columnId, chunkId));
    AFL_VERIFY(it != Chunks.end())("column_id", columnId)("chunk_idx", chunkId);
    return it->second->GetData();
}

}
