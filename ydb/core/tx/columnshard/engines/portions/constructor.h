#pragma once
#include "index_chunk.h"
#include "column_record.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <ydb/core/formats/arrow/special_keys.h>

namespace NKikimr::NOlap {
class TPortionInfo;
class TVersionedIndex;
class ISnapshotSchema;
class TPortionInfoConstructor;

class TPortionMetaConstructor {
private:
    std::optional<NArrow::TFirstLastSpecialKeys> FirstAndLastPK;
    std::optional<TString> TierName;
    std::optional<NStatistics::TPortionStorage> StatisticsStorage;
    std::optional<TSnapshot> RecordSnapshotMin;
    std::optional<TSnapshot> RecordSnapshotMax;
    std::optional<NPortion::EProduced> Produced;
    friend class TPortionInfoConstructor;
    void FillMetaInfo(const NArrow::TFirstLastSpecialKeys& primaryKeys, const NArrow::TMinMaxSpecialKeys& snapshotKeys, const TIndexInfo& indexInfo) {
        AFL_VERIFY(!FirstAndLastPK);
        FirstAndLastPK = *primaryKeys.BuildAccordingToSchemaVerified(indexInfo.GetReplaceKey());
        AFL_VERIFY(!RecordSnapshotMin);
        AFL_VERIFY(!RecordSnapshotMax);
        {
            auto cPlanStep = snapshotKeys.GetBatch()->GetColumnByName(TIndexInfo::SPEC_COL_PLAN_STEP);
            auto cTxId = snapshotKeys.GetBatch()->GetColumnByName(TIndexInfo::SPEC_COL_TX_ID);
            Y_ABORT_UNLESS(cPlanStep && cTxId);
            Y_ABORT_UNLESS(cPlanStep->type_id() == arrow::UInt64Type::type_id);
            Y_ABORT_UNLESS(cTxId->type_id() == arrow::UInt64Type::type_id);
            const arrow::UInt64Array& cPlanStepArray = static_cast<const arrow::UInt64Array&>(*cPlanStep);
            const arrow::UInt64Array& cTxIdArray = static_cast<const arrow::UInt64Array&>(*cTxId);
            RecordSnapshotMin = TSnapshot(cPlanStepArray.GetView(0), cTxIdArray.GetView(0));
            RecordSnapshotMax = TSnapshot(cPlanStepArray.GetView(snapshotKeys.GetBatch()->num_rows() - 1), cTxIdArray.GetView(snapshotKeys.GetBatch()->num_rows() - 1));
        }
    }

public:
    TPortionMetaConstructor() = default;
    TPortionMetaConstructor(const TPortionMeta& meta) {
        FirstAndLastPK = meta.ReplaceKeyEdges;
        RecordSnapshotMin = meta.RecordSnapshotMin;
        RecordSnapshotMax = meta.RecordSnapshotMax;
        TierName = meta.GetTierNameOptional();
        if (!meta.StatisticsStorage.IsEmpty()) {
            StatisticsStorage = meta.StatisticsStorage;
        }
        if (meta.Produced != NPortion::EProduced::UNSPECIFIED) {
            Produced = meta.Produced;
        }
    }

    void SetTierName(const TString& tierName) {
        AFL_VERIFY(!TierName);
        if (!tierName || tierName == IStoragesManager::DefaultStorageId) {
            TierName.reset();
        } else {
            TierName = tierName;
        }
    }

    void SetStatisticsStorage(NStatistics::TPortionStorage&& storage) {
        AFL_VERIFY(!StatisticsStorage);
        StatisticsStorage = std::move(storage);
    }

    void ResetStatisticsStorage(NStatistics::TPortionStorage&& storage) {
        StatisticsStorage = std::move(storage);
    }

    void UpdateRecordsMeta(const NPortion::EProduced prod) {
        Produced = prod;
    }

    TPortionMeta Build() {
        AFL_VERIFY(FirstAndLastPK);
        AFL_VERIFY(RecordSnapshotMin);
        AFL_VERIFY(RecordSnapshotMax);
        TPortionMeta result(*FirstAndLastPK, *RecordSnapshotMin, *RecordSnapshotMax);
        if (TierName) {
            result.TierName = *TierName;
        }
        AFL_VERIFY(Produced);
        result.Produced = *Produced;
        if (StatisticsStorage) {
            result.StatisticsStorage = *StatisticsStorage;
        }
        return result;
    }

    [[nodiscard]] bool LoadMetadata(const NKikimrTxColumnShard::TIndexPortionMeta& portionMeta, const TIndexInfo& indexInfo) {
        if (!!Produced) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DeserializeFromProto")("error", "parsing duplication");
            return true;
        }
        if (portionMeta.HasStatisticsStorage()) {
            auto parsed = NStatistics::TPortionStorage::BuildFromProto(portionMeta.GetStatisticsStorage());
            if (!parsed) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DeserializeFromProto")("error", parsed.GetErrorMessage());
                return false;
            }
            StatisticsStorage = parsed.DetachResult();
            if (StatisticsStorage->IsEmpty()) {
                StatisticsStorage.reset();
            }
        }
        if (portionMeta.GetTierName()) {
            TierName = portionMeta.GetTierName();
        }
        if (portionMeta.GetIsInserted()) {
            Produced = TPortionMeta::EProduced::INSERTED;
        } else if (portionMeta.GetIsCompacted()) {
            Produced = TPortionMeta::EProduced::COMPACTED;
        } else if (portionMeta.GetIsSplitCompacted()) {
            Produced = TPortionMeta::EProduced::SPLIT_COMPACTED;
        } else if (portionMeta.GetIsEvicted()) {
            Produced = TPortionMeta::EProduced::EVICTED;
        } else {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DeserializeFromProto")("error", "incorrect portion meta")("meta", portionMeta.DebugString());
            return false;
        }
        AFL_VERIFY(Produced != TPortionMeta::EProduced::UNSPECIFIED);
        AFL_VERIFY(portionMeta.HasPrimaryKeyBorders());
        FirstAndLastPK = NArrow::TFirstLastSpecialKeys(portionMeta.GetPrimaryKeyBorders(), indexInfo.GetReplaceKey());

        AFL_VERIFY(portionMeta.HasRecordSnapshotMin());
        RecordSnapshotMin = TSnapshot(portionMeta.GetRecordSnapshotMin().GetPlanStep(), portionMeta.GetRecordSnapshotMin().GetTxId());
        AFL_VERIFY(portionMeta.HasRecordSnapshotMax());
        RecordSnapshotMax = TSnapshot(portionMeta.GetRecordSnapshotMax().GetPlanStep(), portionMeta.GetRecordSnapshotMax().GetTxId());
        return true;
    }

};

class TPortionInfoConstructor {
private:
    YDB_ACCESSOR(ui64, PathId, 0);
    std::optional<ui64> PortionId;

    TPortionMetaConstructor MetaConstructor;

    std::optional<TSnapshot> MinSnapshotDeprecated;
    std::optional<TSnapshot> RemoveSnapshot;
    std::optional<ui64> SchemaVersion;

    std::vector<TIndexChunk> Indexes;
    YDB_ACCESSOR_DEF(std::vector<TColumnRecord>, Records);
    std::vector<TUnifiedBlobId> BlobIds;
public:
    void SetPortionId(const ui64 value) {
        AFL_VERIFY(value);
        PortionId = value;
    }

    void AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch) {
        Y_ABORT_UNLESS(batch->num_rows() == GetRecordsCount());
        MetaConstructor.FillMetaInfo(NArrow::TFirstLastSpecialKeys(batch),
            NArrow::TMinMaxSpecialKeys(batch, TIndexInfo::ArrowSchemaSnapshot()), snapshotSchema.GetIndexInfo());
    }

    void AddMetadata(const ISnapshotSchema& snapshotSchema, const NArrow::TFirstLastSpecialKeys& firstLastRecords, const NArrow::TMinMaxSpecialKeys& minMaxSpecial) {
        MetaConstructor.FillMetaInfo(firstLastRecords, minMaxSpecial, snapshotSchema.GetIndexInfo());
    }

    ui64 GetPortionIdVerified() const {
        AFL_VERIFY(PortionId);
        AFL_VERIFY(*PortionId);
        return *PortionId;
    }

    TPortionMetaConstructor& MutableMeta() {
        return MetaConstructor;
    }

    const TPortionMetaConstructor& GetMeta() const {
        return MetaConstructor;
    }

    TPortionInfoConstructor(const TPortionInfo& portion, const bool withBlobs, const bool withMetadata)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , MinSnapshotDeprecated(portion.GetMinSnapshotDeprecated())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionOptional())
    {
        if (withMetadata) {
            MetaConstructor = TPortionMetaConstructor(portion.Meta);
        }
        if (withBlobs) {
            Indexes = portion.GetIndexes();
            Records = portion.GetRecords();
            BlobIds = portion.BlobIds;
        }
    }

    TPortionInfoConstructor(TPortionInfo&& portion)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , MinSnapshotDeprecated(portion.GetMinSnapshotDeprecated())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionOptional())
    {
        MetaConstructor = TPortionMetaConstructor(portion.Meta);
        
        Indexes = std::move(portion.Indexes);
        Records = std::move(portion.Records);
        BlobIds = std::move(portion.BlobIds);
    }

    TPortionAddress GetAddress() const {
        return TPortionAddress(PathId, GetPortionIdVerified());
    }

    bool HasRemoveSnapshot() const {
        return !!RemoveSnapshot;
    }

    template <class TChunkInfo>
    static void CheckChunksOrder(const std::vector<TChunkInfo>& chunks) {
        ui32 entityId = 0;
        ui32 chunkIdx = 0;
        for (auto&& i : chunks) {
            if (entityId != i.GetEntityId()) {
                AFL_VERIFY(entityId < i.GetEntityId());
                AFL_VERIFY(i.GetChunkIdx() == 0);
                entityId = i.GetEntityId();
                chunkIdx = 0;
            } else {
                AFL_VERIFY(i.GetChunkIdx() == chunkIdx + 1);
                chunkIdx = i.GetChunkIdx();
            }
            AFL_VERIFY(i.GetEntityId());
        }
    }

    void Merge(TPortionInfoConstructor&& item) {
        AFL_VERIFY(item.PathId == PathId);
        AFL_VERIFY(item.PortionId == PortionId);
        if (item.MinSnapshotDeprecated) {
            if (MinSnapshotDeprecated) {
                AFL_VERIFY(*MinSnapshotDeprecated == *item.MinSnapshotDeprecated);
            } else {
                MinSnapshotDeprecated = item.MinSnapshotDeprecated;
            }
        }
        if (item.RemoveSnapshot) {
            if (RemoveSnapshot) {
                AFL_VERIFY(*RemoveSnapshot == *item.RemoveSnapshot);
            } else {
                RemoveSnapshot = item.RemoveSnapshot;
            }
        }
    }

    TPortionInfoConstructor(const ui64 pathId, const ui64 portionId)
        : PathId(pathId)
        , PortionId(portionId) {
        AFL_VERIFY(PathId);
        AFL_VERIFY(PortionId);
    }

    TPortionInfoConstructor(const ui64 pathId)
        : PathId(pathId) {
        AFL_VERIFY(PathId);
    }

    const TSnapshot& GetMinSnapshotDeprecatedVerified() const {
        AFL_VERIFY(!!MinSnapshotDeprecated);
        return *MinSnapshotDeprecated;
    }

    std::shared_ptr<ISnapshotSchema> GetSchema(const TVersionedIndex& index) const;

    void SetMinSnapshotDeprecated(const TSnapshot& snap) {
        Y_ABORT_UNLESS(snap.Valid());
        MinSnapshotDeprecated = snap;
    }

    void SetSchemaVersion(const ui64 version) {
//        AFL_VERIFY(version); engines/ut
        SchemaVersion = version;
    }

    void SetRemoveSnapshot(const TSnapshot& snap) {
        AFL_VERIFY(!RemoveSnapshot);
        if (snap.Valid()) {
            RemoveSnapshot = snap;
        }
    }

    void SetRemoveSnapshot(const ui64 planStep, const ui64 txId) {
        SetRemoveSnapshot(TSnapshot(planStep, txId));
    }

    void LoadRecord(const TIndexInfo& indexInfo, const TColumnChunkLoadContext& loadContext);

    ui32 GetRecordsCount() const {
        ui32 result = 0;
        std::optional<ui32> columnIdFirst;
        for (auto&& i : Records) {
            if (!columnIdFirst || *columnIdFirst == i.ColumnId) {
                result += i.GetMeta().GetNumRows();
                columnIdFirst = i.ColumnId;
            }
        }
        AFL_VERIFY(columnIdFirst);
        return result;
    }

    TBlobRangeLink16::TLinkId RegisterBlobId(const TUnifiedBlobId& blobId) {
        AFL_VERIFY(blobId.IsValid());
        TBlobRangeLink16::TLinkId idx = 0;
        for (auto&& i : BlobIds) {
            if (i == blobId) {
                return idx;
            }
            ++idx;
        }
        BlobIds.emplace_back(blobId);
        return idx;
    }

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
        return linkRange.RestoreRange(GetBlobId(linkRange.GetBlobIdxVerified()));
    }

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        AFL_VERIFY(linkId < BlobIds.size());
        return BlobIds[linkId];
    }

    ui32 GetBlobIdsCount() const {
        return BlobIds.size();
    }

    void RegisterBlobIdx(const TChunkAddress& address, const TBlobRangeLink16::TLinkId blobIdx) {
        for (auto&& i : Records) {
            if (i.GetColumnId() == address.GetEntityId() && i.GetChunkIdx() == address.GetChunkIdx()) {
                i.RegisterBlobIdx(blobIdx);
                return;
            }
        }
        for (auto&& i : Indexes) {
            if (i.GetIndexId() == address.GetEntityId() && i.GetChunkIdx() == address.GetChunkIdx()) {
                i.RegisterBlobIdx(blobIdx);
                return;
            }
        }
        AFL_VERIFY(false)("problem", "portion haven't address for blob registration")("address", address.DebugString());
    }

    TString DebugString() const {
        TStringBuilder sb;
        return sb;
    }

    void ReorderChunks() {
        {
            auto pred = [](const TColumnRecord& l, const TColumnRecord& r) {
                return l.GetAddress() < r.GetAddress();
            };
            std::sort(Records.begin(), Records.end(), pred);
            CheckChunksOrder(Records);
        }
        {
            auto pred = [](const TIndexChunk& l, const TIndexChunk& r) {
                return l.GetAddress() < r.GetAddress();
            };
            std::sort(Indexes.begin(), Indexes.end(), pred);
            CheckChunksOrder(Indexes);
        }
    }

    void FullValidation() const {
        AFL_VERIFY(Records.size());
        CheckChunksOrder(Records);
        CheckChunksOrder(Indexes);
        std::set<ui32> blobIdxs;
        for (auto&& i : Records) {
            blobIdxs.emplace(i.GetBlobRange().GetBlobIdxVerified());
        }
        for (auto&& i : Indexes) {
            blobIdxs.emplace(i.GetBlobRange().GetBlobIdxVerified());
        }
        if (BlobIds.size()) {
            AFL_VERIFY(BlobIds.size() == blobIdxs.size());
            AFL_VERIFY(BlobIds.size() == *blobIdxs.rbegin() + 1);
        } else {
            AFL_VERIFY(blobIdxs.empty());
        }
    }

    void LoadIndex(const TIndexChunkLoadContext& loadContext);

    const TColumnRecord& AppendOneChunkColumn(TColumnRecord&& record);

    void AddIndex(const TIndexChunk& chunk) {
        ui32 chunkIdx = 0;
        for (auto&& i : Indexes) {
            if (i.GetIndexId() == chunk.GetIndexId()) {
                AFL_VERIFY(chunkIdx == i.GetChunkIdx())("index_id", chunk.GetIndexId())("expected", chunkIdx)("real", i.GetChunkIdx());
                ++chunkIdx;
            }
        }
        AFL_VERIFY(chunkIdx == chunk.GetChunkIdx())("index_id", chunk.GetIndexId())("expected", chunkIdx)("real", chunk.GetChunkIdx());
        Indexes.emplace_back(chunk);
    }

    TPortionInfo Build(const bool needChunksNormalization);
};

class TPortionConstructors {
private:
    THashMap<ui64, THashMap<ui64, TPortionInfoConstructor>> Constructors;
public:
    THashMap<ui64, THashMap<ui64, TPortionInfoConstructor>>::iterator begin() {
        return Constructors.begin();
    }

    THashMap<ui64, THashMap<ui64, TPortionInfoConstructor>>::iterator end() {
        return Constructors.end();
    }

    TPortionInfoConstructor* GetConstructorVerified(const ui64 pathId, const ui64 portionId) {
        auto itPathId = Constructors.find(pathId);
        AFL_VERIFY(itPathId != Constructors.end());
        auto itPortionId = itPathId->second.find(portionId);
        AFL_VERIFY(itPortionId != itPathId->second.end());
        return &itPortionId->second;
    }

    TPortionInfoConstructor* AddConstructorVerified(TPortionInfoConstructor&& constructor) {
        const ui64 pathId = constructor.GetPathId();
        const ui64 portionId = constructor.GetPortionIdVerified();
        auto info = Constructors[pathId].emplace(portionId, std::move(constructor));
        AFL_VERIFY(info.second);
        return &info.first->second;
    }

    TPortionInfoConstructor* MergeConstructor(TPortionInfoConstructor&& constructor) {
        const ui64 pathId = constructor.GetPathId();
        const ui64 portionId = constructor.GetPortionIdVerified();
        auto itPathId = Constructors.find(pathId);
        if (itPathId == Constructors.end()) {
            return AddConstructorVerified(std::move(constructor));
        }
        auto itPortionId = itPathId->second.find(portionId);
        if (itPortionId == itPathId->second.end()) {
            return AddConstructorVerified(std::move(constructor));
        }
        itPortionId->second.Merge(std::move(constructor));
        return &itPortionId->second;
    }

};

}